module TableOperations

using Tables

struct TransformsRow{T, F}
    row::T
    funcs::F
end

Base.getproperty(row::TransformsRow, ::Type{T}, col::Int, nm::Symbol) where {T} = (get(getfield(row, 2), col, identity))(getproperty(getfield(row, 1), T, col, nm))
Base.getproperty(row::TransformsRow, nm::Symbol) = (get(getfield(row, 2), nm, identity))(getproperty(getfield(row, 1), nm))

struct Transforms{T, F}
    source::T
    funcs::F # NamedTuple of columnname=>transform function
end

transform(funcs) = x->transform(x, funcs)
transform(src, funcs::NamedTuple{names, types}) where {names, types} = Transforms(src, funcs)

function transform(src, d::Dict{String, <:Function})
    names = Tuple(Symbol(nm) for nm in keys(d))
    return Transforms(source, NamedTuple{names}(Tuple(values(d))))
end

function transform(src, d::Dict{Int, <:Function})
    sch = Tables.schema(src)
    nms = Tables.names(sch)
    names = Tuple(nms[i] for i in keys(d))
    return Transforms(source, NamedTuple{names}(Tuple(values(d))))
end

Base.@pure function tupletype(sch::Type{NamedTuple{names, T}}, funcs::NamedTuple{nms}) where {names, T, nms}
    Tuple{Any[ Base.sym_in(nm, nms) ? Core.Compiler.return_type(funcs[nm], (fieldtype(sch, nm),)) : fieldtype(sch, nm) for nm in names]...}
end

function Tables.schema(t::Transforms)
    sch = Tables.schema(t.source)
    return NamedTuple{Tables.names(sch), tupletype(sch, t.funcs)}
end

struct TransformsRows{T, F}
    source::T
    funcs::F
end

Tables.rows(t::Transforms) = TransformsRows(Tables.rows(t.source), t.funcs)

Base.IteratorSize(::Type{TransformsRows{T, F}}) where {T, F} = Base.IteratorSize(T)
Base.length(t::TransformsRows) = length(t.source)
Base.eltype(t::TransformsRows{T, F}) where {T, F} = TransformsRow{eltype(t.source), F}

function Base.iterate(t::TransformsRows, st=())
    state = iterate(t.source, st...)
    state === nothing && return nothing
    return TransformsRow(state[1], t.funcs), (state[2],)
end

end # module
