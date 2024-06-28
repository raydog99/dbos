module BTreeGeneric = struct
  type config = {
    use_bulk_insert : bool;
    enable_wal : bool;
  }

  type t = {
    mutable dt_id : int;
    mutable config : config;
    mutable meta_node_bf : buffer_frame;
    mutable height : int;
  }

  type separator_info = {
    length : int;
    slot : int;
    is_underflow : bool;
  }

  let create dtid config =
    let tree = {
      dt_id = dtid;
      config = config;
      meta_node_bf = BufferManager.allocate_page ();
      height = 1;
    } in
    let meta_node = tree.meta_node_bf in
    BufferManager.set_keep_in_memory meta_node true;
    BufferManager.set_dt_id meta_node dtid;
    
    let root_node = BufferManager.allocate_page () in
    BufferManager.init_as_leaf root_node;
    
    BufferManager.set_upper meta_node root_node;
    
    BufferManager.increment_gsn root_node;
    BufferManager.increment_gsn meta_node;
    tree

  let try_split tree to_split favored_split_pos =
    Wal.ensure_enough_space (BufferManager.page_size * 1);

    let parent_handler = find_parent_eager tree to_split in
    let p_guard = parent_handler.get_parent_read_page_guard () in
    let c_guard = BufferManager.get_child_guard p_guard parent_handler.swip in

    if BufferManager.get_count c_guard <= 1 then
      ()
    else
      let sep_info =
        if favored_split_pos < 0 || favored_split_pos >= BufferManager.get_count c_guard - 1 then
          if tree.config.use_bulk_insert then
            let pos = BufferManager.get_count c_guard - 2 in
            { length = BufferManager.get_full_key_len c_guard pos; slot = pos; is_underflow = false }
          else
            BufferManager.find_sep c_guard
        else
          { length = BufferManager.get_full_key_len c_guard favored_split_pos; slot = favored_split_pos; is_underflow = false }
      in
      
      let sep_key = BufferManager.get_sep c_guard sep_info in

      if BufferManager.is_meta_node p_guard then
        let p_x_guard = BufferManager.upgrade_to_exclusive p_guard in
        let c_x_guard = BufferManager.upgrade_to_exclusive c_guard in
        
        let new_root = BufferManager.allocate_page () in
        let new_left_node = BufferManager.allocate_page () in

        if tree.config.enable_wal then begin
          BufferManager.increment_gsn new_root;
          BufferManager.increment_gsn new_left_node;
          BufferManager.increment_gsn c_x_guard;
        end else begin
          BufferManager.mark_as_dirty new_root;
          BufferManager.mark_as_dirty new_left_node;
          BufferManager.mark_as_dirty c_x_guard;
        end;

        BufferManager.init_as_internal new_root;
        BufferManager.set_upper new_root c_x_guard;
        BufferManager.set_upper p_x_guard new_root;

        BufferManager.init new_left_node (BufferManager.is_leaf c_x_guard);
        BufferManager.split c_x_guard new_root new_left_node sep_info.slot sep_key;

        tree.height <- tree.height + 1;
      else
        let space_needed = BufferManager.space_needed p_guard sep_info.length in
        if BufferManager.has_enough_space p_guard space_needed then begin
          let p_x_guard = BufferManager.upgrade_to_exclusive p_guard in
          let c_x_guard = BufferManager.upgrade_to_exclusive c_guard in

          BufferManager.request_space p_x_guard space_needed;

          let new_left_node = BufferManager.allocate_page () in

          if tree.config.enable_wal then begin
            BufferManager.increment_gsn p_x_guard;
            BufferManager.increment_gsn new_left_node;
            BufferManager.increment_gsn c_x_guard;
          end else begin
            BufferManager.mark_as_dirty p_x_guard;
            BufferManager.mark_as_dirty new_left_node;
            BufferManager.mark_as_dirty c_x_guard;
          end;

          BufferManager.init new_left_node (BufferManager.is_leaf c_x_guard);
          BufferManager.split c_x_guard p_x_guard new_left_node sep_info.slot sep_key;

        end else begin
          BufferManager.unlock p_guard;
          BufferManager.unlock c_guard;
          try_split tree p_guard (-1)
        end
end