module TableOperations

using Tables

struct TransformsRow{T, F}
    row::T
    funcs::F
end

Base.getproperty(row::TransformsRow, ::Type{T}, col::Int, nm::Symbol) where {T} = (getfunc(getfield(row, 2), col, nm))(getproperty(getfield(row, 1), T, col, nm))
Base.getproperty(row::TransformsRow, nm::Symbol) = getproperty(row, Any, Tables.columnindex(propertynames(row), nm), nm)
Base.propertynames(row::TransformsRow) = propertynames(getfield(row, 1))

struct Transforms{T, F}
    source::T
    funcs::F # NamedTuple of columnname=>transform function
end

transform(funcs) = x->transform(x, funcs)
transform(src, funcs::NamedTuple{names, types}) where {names, types} = Transforms(Tables.rows(src), funcs)
transform(src, d::Dict{String, <:Function}) = Transforms(Tables.rows(src), d)
transform(src, d::Dict{Int, <:Function}) = Transforms(Tables.rows(src), d)

getfunc(nt::NamedTuple, i, nm, default=identity) = nt[i]
getfunc(d::Dict{String, <:Function}, i, nm, default=identity) = d[nm]
getfunc(d::Dict{String, <:Function}, i, nm, default=identity) = d[i]

Tables.istable(::Type{<:Transforms}) = true
Tables.rowaccess(::Type{<:Transforms}) = true
Tables.rows(t::Transforms) = t
# vaoid relying on inference here and just let sinks figure things out
Tables.schema(t::Transforms) = nothing

Base.IteratorSize(::Type{Transforms{T}}) where {T} = Base.IteratorSize(T)
Base.length(t::Transforms) = length(t.source)
Base.eltype(t::Transforms{T, F}) where {T, F} = TransformsRow{eltype(t.source), F}

function Base.iterate(t::Transforms, st=())
    state = iterate(t.source, st...)
    state === nothing && return nothing
    return TransformsRow(state[1], t.funcs), (state[2],)
end

# select
struct Select{T, columnaccess, names}
    source::T
end

function select(x::T, names::Symbol...) where {T}
    columnaccess = Tables.columnaccess(T)
    r = columnaccess ? Tables.columns(x) : Tables.rows(x)
    return Select{typeof(r), columnaccess, names}(r)
end

Tables.istable(::Type{<:Select}) = true

Base.@pure function typesubset(::Tables.Schema{names, types}, nms) where {names, types}
    return Tuple{Any[Tables.columntype(names, types, nm) for nm in nms]...}
end

function Tables.schema(s::Select{T, columnaccess, names}) where {T, columnaccess, names}
    sch = Tables.schema(getfield(s, 1))
    sch === nothing && return nothing
    return Tables.Schema(names, typesubset(sch, names))
end

# Tables.columns: make Select property-accessible
Base.getproperty(s::Select, nm::Symbol) = getproperty(getfield(s, 1), nm)
Base.propertynames(s::Select{T, names}) where {T, names} = names
Tables.columnaccess(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = columnaccess
Tables.columns(s::Select{T, columnaccess, names}) where {T, columnaccess, names} = columnaccess ? s :
    Tables.buildcolumns(Tables.schema(s), s)

# Tables.rows: implement Iterator interface
Base.IteratorSize(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = Base.IteratorSize(T)
Base.length(s::Select) = length(getfield(s, 1))
Base.IteratorEltype(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = Base.IteratorEltype(T)
Base.eltype(s::Select{T, columnaccess, names}) where {T, columnaccess, names} = SelectRow{eltype(getfield(s, 1)), names}
Tables.rowaccess(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = !columnaccess
Tables.rows(s::Select{T, columnaccess, names}) where {T, columnaccess, names} = columnaccess ? Tables.RowIterator(s, Tables.rowcount(getfield(s, 1))) : s

# we need to iterate a "row view" in case the underlying source has unknown schema
# to ensure each iterated row only has `names` propertynames
struct SelectRow{T, names}
    row::T
end

Base.getproperty(row::SelectRow, ::Type{T}, col::Int, nm::Symbol) where {T} = getproperty(getfield(row, 1), T, col, nm)
Base.getproperty(row::SelectRow, nm::Symbol) = getproperty(getfield(row, 1), nm)
Base.propertynames(row::SelectRow{T, names}) where {T, names} = names

function Base.iterate(s::Select{T, names}, st=()) where {T, names}
    state = iterate(getfield(s, 1), st...)
    state === nothing && return nothing
    row, st = state
    return SelectRow{typeof(row), names}(row), (st,)
end

# map
struct Map{T, F}
    source::T
    func::F
end

Tables.istable(::Type{<:Map}) = true
Tables.rowaccess(::Type{<:Map}) = true
Tables.rows(m::Map) = m
Tables.schema(m::Map) = nothing

Base.IteratorSize(::Type{Map{T, F}}) where {T, F} = Base.IteratorSize(T)
Base.length(m::Map) = length(m.source)
Base.IteratorEltype(::Type{<:Map}) = Base.EltypeUnknown()

function Base.iterate(m::Map, st=())
    state = iterate(m.source, st...)
    state === nothing && return nothing
    return m.func(state[1]), (state[2],)
end

struct Filter{T, F}
    source::T
    func::F
end

Tables.istable(::Type{<:Filter}) = true
Tables.rowaccess(::Type{<:Filter}) = true
Tables.rows(f::Filter) = f
Tables.schema(f::Filter) = Tables.schema(f.source)

Base.IteratorSize(::Type{Filter{T, F}}) where {T, F} = Base.IteratorSize(T)
Base.length(f::Filter) = length(f.source)
Base.eltype(f::Filter{T, F}) where {T, F} = eltype(f.source)

function Base.iterate(f::Filter, st=())
    state = iterate(f.source, st...)
    state === nothing && return nothing
    row, st = state
    while !f.func(row)
        state = iterate(f.source, st)
        state === nothing && return nothing
        row, st = state
    end
    return row, (st,)
end

end # module
