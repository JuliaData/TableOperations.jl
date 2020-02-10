module TableOperations

using Tables

struct TransformsRow{T, F} <: Tables.AbstractRow
    row::T
    funcs::F
end

getrow(r::TransformsRow) = getfield(r, :row)
getfuncs(r::TransformsRow) = getfield(r, :funcs)

Tables.getcolumn(row::TransformsRow, nm::Symbol) = (getfunc(row, getfuncs(row), nm))(Tables.getcolumn(getrow(row), nm))
Tables.getcolumn(row::TransformsRow, i::Int) = (getfunc(row, getfuncs(row), i))(Tables.getcolumn(getrow(row), i))
Tables.columnnames(row::TransformsRow) = Tables.columnnames(getrow(row))

struct Transforms{C, T, F}
    source::T
    funcs::F # NamedTuple of columnname=>transform function
end

Tables.columnnames(t::Transforms{true}) = Tables.columnnames(getfield(t, 1))
Tables.getcolumn(t::Transforms{true}, nm::Symbol) = Base.map(getfunc(t, getfield(t, 2), nm), Tables.getcolumn(getfield(t, 1), nm))
Tables.getcolumn(t::Transforms{true}, i::Int) = Base.map(getfunc(t, getfield(t, 2), i), Tables.getcolumn(getfield(t, 1), i))
# for backwards compat
Base.propertynames(t::Transforms{true}) = Tables.columnnames(t)
Base.getproperty(t::Transforms{true}, nm::Symbol) = Tables.getcolumn(t, nm)

"""
    Tables.transform(source, funcs) => Tables.Transforms
    source |> Tables.transform(funcs) => Tables.Transform

Given any Tables.jl-compatible source, apply a series of transformation functions, for the columns specified in `funcs`.
The tranform functions can be a NamedTuple or Dict mapping column name (`String` or `Symbol` or `Integer` index) to Function.
"""
function transform end

transform(funcs) = x->transform(x, funcs)
transform(; kw...) = transform(kw.data)
function transform(src::T, funcs::F) where {T, F}
    C = Tables.columnaccess(T)
    x = C ? Tables.columns(src) : Tables.rows(src)
    return Transforms{C, typeof(x), F}(x, funcs)
end

getfunc(row, nt::NamedTuple, nm::Symbol) = get(nt, nm, identity)
getfunc(row, d::Dict{String, <:Base.Callable}, nm::Symbol) = get(d, String(nm), identity)
getfunc(row, d::Dict{Symbol, <:Base.Callable}, nm::Symbol) = get(d, nm, identity)
getfunc(row, d::Dict{Int, <:Base.Callable}, nm::Symbol) = get(d, findfirst(isequal(nm), Tables.columnnames(row)), identity)

getfunc(row, nt::NamedTuple, i::Int) = get(nt, Tables.columnnames(row)[i], identity)
getfunc(row, d::Dict{String, <:Base.Callable}, i::Int) = get(d, String(Tables.columnnames(row)[i]), identity)
getfunc(row, d::Dict{Symbol, <:Base.Callable}, i::Int) = get(d, Tables.columnnames(row)[i], identity)
getfunc(row, d::Dict{Int, <:Base.Callable}, i::Int) = get(d, i, identity)

Tables.istable(::Type{<:Transforms}) = true
Tables.rowaccess(::Type{Transforms{C, T, F}}) where {C, T, F} = !C
Tables.rows(t::Transforms{false}) = t
Tables.columnaccess(::Type{Transforms{C, T, F}}) where {C, T, F} = C
Tables.columns(t::Transforms{true}) = t
# avoid relying on inference here and just let sinks figure things out
Tables.schema(t::Transforms) = nothing

Base.IteratorSize(::Type{Transforms{false, T, F}}) where {T, F} = Base.IteratorSize(T)
Base.length(t::Transforms{false}) = length(getfield(t, 1))
Base.eltype(t::Transforms{false, T, F}) where {T, F} = TransformsRow{eltype(getfield(t, 1)), F}

@inline function Base.iterate(t::Transforms{false}, st=())
    state = iterate(getfield(t, 1), st...)
    state === nothing && return nothing
    return TransformsRow(state[1], getfield(t, 2)), (state[2],)
end

# select
struct Select{T, columnaccess, names}
    source::T
end

"""
    Tables.select(source, columns...) => Tables.Select
    source |> Tables.select(columns...) => Tables.Select

Create a lazy wrapper that satisfies the Tables.jl interface and keeps only the columns given by the columns arguments, which can be `String`s, `Symbol`s, or `Integer`s
"""
function select end

select(names::Symbol...) = x->select(x, names...)
select(names::String...) = x->select(x, Base.map(Symbol, names)...)
select(inds::Integer...) = x->select(x, Base.map(Int, inds)...)

function select(x::T, names...) where {T}
    colaccess = Tables.columnaccess(T)
    r = colaccess ? Tables.columns(x) : Tables.rows(x)
    return Select{typeof(r), colaccess, names}(r)
end

Tables.istable(::Type{<:Select}) = true

Base.@pure function typesubset(::Tables.Schema{names, types}, nms::NTuple{N, Symbol}) where {names, types, N}
    return Tuple{Any[Tables.columntype(names, types, nm) for nm in nms]...}
end

Base.@pure function typesubset(::Tables.Schema{names, types}, inds::NTuple{N, Int}) where {names, types, N}
    return Tuple{Any[fieldtype(types, i) for i in inds]...}
end

typesubset(::Tables.Schema{names, types}, ::Tuple{}) where {names, types} = Tuple{}

namesubset(::Tables.Schema{names, types}, nms::NTuple{N, Symbol}) where {names, types, N} = nms
Base.@pure namesubset(::Tables.Schema{names, T}, inds::NTuple{N, Int}) where {names, T, N} = ntuple(i -> names[inds[i]], N)
namesubset(::Tables.Schema{names, types}, ::Tuple{}) where {names, types} = ()
namesubset(names, nms::NTuple{N, Symbol}) where {N} = nms
namesubset(names, inds::NTuple{N, Int}) where {N} = ntuple(i -> names[inds[i]], N)
namesubset(names, ::Tuple{}) = ()

function Tables.schema(s::Select{T, columnaccess, names}) where {T, columnaccess, names}
    sch = Tables.schema(getfield(s, 1))
    sch === nothing && return nothing
    return Tables.Schema(namesubset(sch, names), typesubset(sch, names))
end

# columns: make Select property-accessible
Tables.getcolumn(s::Select{T, true, names}, nm::Symbol) where {T, names} = Tables.getcolumn(getfield(s, 1), nm)
Tables.getcolumn(s::Select{T, true, names}, i::Int) where {T, names} = Tables.getcolumn(getfield(s, 1), i)
Tables.columnnames(s::Select{T, true, names}) where {T, names} = namesubset(Tables.columnnames(getfield(s, 1)), names)
Tables.columnaccess(::Type{Select{T, C, names}}) where {T, C, names} = C
Tables.columns(s::Select{T, true, names}) where {T, names} = s
# for backwards compat
Base.propertynames(s::Select{T, true, names}) where {T, names} = Tables.columnnames(s)
Base.getproperty(s::Select{T, true, names}, nm::Symbol) where {T, names} = Tables.getcolumn(s, nm)

# rows: implement Iterator interface
Base.IteratorSize(::Type{Select{T, false, names}}) where {T, names} = Base.IteratorSize(T)
Base.length(s::Select{T, false, names}) where {T, names} = length(getfield(s, 1))
Base.IteratorEltype(::Type{Select{T, false, names}}) where {T, names} = Base.IteratorEltype(T)
Base.eltype(s::Select{T, false, names}) where {T, names} = SelectRow{eltype(getfield(s, 1)), names}
Tables.rowaccess(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = !columnaccess
Tables.rows(s::Select{T, false, names}) where {T, names} = s

# we need to iterate a "row view" in case the underlying source has unknown schema
# to ensure each iterated row only has `names` Tables.columnnames
struct SelectRow{T, names} <: Tables.AbstractRow
    row::T
end

Tables.getcolumn(row::SelectRow, nm::Symbol) = Tables.getcolumn(getfield(row, 1), nm)
Tables.getcolumn(row::SelectRow, i::Int) = Tables.getcolumn(getfield(row, 1), i)
Tables.getcolumn(row::SelectRow, ::Type{T}, i::Int, nm::Symbol) where {T} = Tables.getcolumn(getfield(row, 1), T, i, nm)

getprops(row, nms::NTuple{N, Symbol}) where {N} = nms
getprops(row, inds::NTuple{N, Int}) where {N} = ntuple(i->Tables.columnnames(getfield(row, 1))[inds[i]], N)
getprops(row, ::Tuple{}) = ()

Tables.columnnames(row::SelectRow{T, names}) where {T, names} = getprops(row, names)

@inline function Base.iterate(s::Select{T, false, names}) where {T, names}
    state = iterate(getfield(s, 1))
    state === nothing && return nothing
    row, st = state
    return SelectRow{typeof(row), names}(row), st
end

@inline function Base.iterate(s::Select{T, false, names}, st) where {T, names}
    state = iterate(getfield(s, 1), st)
    state === nothing && return nothing
    row, st = state
    return SelectRow{typeof(row), names}(row), st
end

# filter
struct Filter{F, T}
    f::F
    x::T
end

"""
    Tables.filter(f, source) => Tables.Filter
    source |> Tables.filter(f) => Tables.Filter

Create a lazy wrapper that satisfies the Tables.jl interface and keeps the rows where `f(row)` is true.
"""
function filter end

function filter(f::F, x) where {F <: Base.Callable}
    r = Tables.rows(x)
    return Filter{F, typeof(r)}(f, r)
end
filter(f::Base.Callable) = x->filter(f, x)

Tables.isrowtable(::Type{<:Filter}) = true
Tables.schema(f::Filter) = Tables.schema(f.x)

Base.IteratorSize(::Type{Filter{F, T}}) where {F, T} = Base.SizeUnknown()
Base.IteratorEltype(::Type{Filter{F, T}}) where {F, T} = Base.IteratorEltype(T)
Base.eltype(f::Filter) = eltype(f.x)

 @inline function Base.iterate(f::Filter)
    state = iterate(f.x)
    state === nothing && return nothing
    while !f.f(state[1])
        state = iterate(f.x, state[2])
        state === nothing && return nothing
    end
    return state
end

 @inline function Base.iterate(f::Filter, st)
    state = iterate(f.x, st)
    state === nothing && return nothing
    while !f.f(state[1])
        state = iterate(f.x, state[2])
        state === nothing && return nothing
    end
    return state
end

# map
struct Map{F, T}
    func::F
    source::T
end

"""
    Tables.map(f, source) => Tables.Map
    source |> Tables.map(f) => Tables.Map

Create a lazy wrapper that satisfies the Tables.jl interface and will apply the function `f(row)` to each
row in the input table source. Note that `f` must take and produce a valid Tables.jl `Row` object.
"""
function map end

function map(f::F, x::T) where {F <: Base.Callable, T}
    r = Tables.rows(x)
    return Map{F, typeof(r)}(f, r)
end
map(f::Base.Callable) = x->map(f, x)

Tables.isrowtable(::Type{<:Map}) = true
Tables.schema(m::Map) = nothing

Base.IteratorSize(::Type{Map{T, F}}) where {T, F} = Base.IteratorSize(T)
Base.length(m::Map) = length(m.source)
Base.IteratorEltype(::Type{<:Map}) = Base.EltypeUnknown()

@inline function Base.iterate(m::Map)
    state = iterate(m.source)
    state === nothing && return nothing
    return m.func(state[1]), state[2]
end

@inline function Base.iterate(m::Map, st)
    state = iterate(m.source, st)
    state === nothing && return nothing
    return m.func(state[1]), state[2]
end
end # module
