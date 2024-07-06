open Core

module CatalogEntryHashFunction : sig
  type t = Catalog_entry.t -> int
  val create : unit -> t
end

module CatalogEntryEquality : sig
  type t = Catalog_entry.t -> Catalog_entry.t -> bool
  val create : unit -> t
end

module CatalogEntrySet : sig
  include Set.S with type Elt.t = Catalog_entry.t

  val create : ?hash_func:Catalog_entry_hash_function.t ->
               ?equality:Catalog_entry_equality.t ->
               unit -> t
end

module CatalogEntryMap : sig
  include Map.S with type Key.t = Catalog_entry.t

  val create : ?hash_func:Catalog_entry_hash_function.t ->
               ?equality:Catalog_entry_equality.t ->
               unit -> 'a t
end

module CatalogEntryVector : sig
  type t = Catalog_entry.t list

  val create : unit -> t
  val add : t -> Catalog_entry.t -> t
  val remove : t -> Catalog_entry.t -> t
  val mem : t -> Catalog_entry.t -> bool
  val length : t -> int
  val iter : t -> f:(Catalog_entry.t -> unit) -> unit
  val fold : t -> init:'a -> f:('a -> Catalog_entry.t -> 'a) -> 'a
  val to_list : t -> Catalog_entry.t list
end