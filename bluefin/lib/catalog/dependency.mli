open Core
open CatalogType
open CatalogEntry
open Serializer
open Deserializer

module DependencyFlags : sig
  type t

  val create : unit -> t
  val copy : t -> t
  val equal : t -> t -> bool
  val to_string : t -> string
end

module DependencySubjectFlags : sig
  include module type of DependencyFlags

  val apply : t -> t -> t
  val is_ownership : t -> bool
  val set_ownership : t -> t
end

module DependencyDependentFlags : sig
  include module type of DependencyFlags

  val apply : t -> t -> t
  val is_blocking : t -> bool
  val is_owned_by : t -> bool
  val set_blocking : t -> t
  val set_owned_by : t -> t
end

module CatalogEntryInfo : sig
  type t = {
    type_ : CatalogType.t;
    schema : string;
    name : string;
  }

  val equal : t -> t -> bool
  val serialize : t -> Serializer.t -> unit
  val deserialize : Deserializer.t -> t
end

module Dependency : sig
  type t = {
    entry : CatalogEntry.t;
    flags : DependencyDependentFlags.t;
  }

  val create : CatalogEntry.t -> ?flags:DependencyDependentFlags.t -> unit -> t
end

module DependencySet : sig
  type t

  val create : unit -> t
  val add : t -> Dependency.t -> unit
  val remove : t -> Dependency.t -> unit
  val mem : t -> Dependency.t -> bool
  val iter : t -> f:(Dependency.t -> unit) -> unit
  val fold : t -> init:'a -> f:('a -> Dependency.t -> 'a) -> 'a
end