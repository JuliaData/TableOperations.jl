using TableOperations, Tables, Test

ctable = (A=[1, missing, 3], B=[1.0, 2.0, 3.0], C=["hey", "there", "sailor"])
rtable = Tables.rowtable(ctable)
rtable2 = Iterators.filter(i -> i.a % 2 == 0, [(a=x, b=y) for (x, y) in zip(1:20, 21:40)])

struct ReallyWideTable
end
Tables.istable(::Type{ReallyWideTable}) = true
Tables.columnaccess(::Type{ReallyWideTable}) = true
Tables.columns(x::ReallyWideTable) = x
Tables.columnnames(x::ReallyWideTable) = [Symbol(:x, i) for i = 1:100_000]
Tables.getcolumn(x::ReallyWideTable, i::Int) = rand(10)
Tables.getcolumn(x::ReallyWideTable, nm::Symbol) = rand(10)
Tables.schema(x::ReallyWideTable) = Tables.Schema(Tables.columnnames(x), [Float64 for _ = 1:100_000])

@testset "TableOperations.transform" begin

tran = ctable |> TableOperations.transform(C=Symbol)
@test Tables.istable(typeof(tran))
@test !Tables.rowaccess(typeof(tran))
@test Tables.columnaccess(typeof(tran))
@test Tables.columns(tran) === tran
@test isequal(Tables.getcolumn(tran, :A), [1,missing,3])
@test isequal(Tables.getcolumn(tran, 1), [1,missing,3])

tran2 = rtable |> TableOperations.transform(C=Symbol)
@test Tables.istable(typeof(tran2))
@test Tables.rowaccess(typeof(tran2))
@test !Tables.columnaccess(typeof(tran2))
@test Tables.rows(tran2) === tran2
@test Base.IteratorSize(typeof(tran2)) == Base.HasShape{1}()
@test length(tran2) == 3
@test eltype(tran2) == TableOperations.TransformsRow{NamedTuple{(:A, :B, :C),Tuple{Union{Missing, Int},Float64,String}},NamedTuple{(:C,),Tuple{DataType}}}
trow = first(tran2)
@test trow.A === 1
@test trow.B === 1.0
@test trow.C == :hey
@test Tables.getcolumn(trow, 1) == 1
@test Tables.getcolumn(trow, :A) == 1
ctable2 = Tables.columntable(tran2)
@test isequal(ctable2.A, ctable.A)
@test ctable2.C == map(Symbol, ctable.C)

# test various ways of inputting TableOperations.transform functions
table = TableOperations.transform(ctable, Dict{String, Base.Callable}("C" => Symbol)) |> Tables.columntable
@test table.C == [:hey, :there, :sailor]

table = ctable |> TableOperations.transform(C=Symbol) |> Tables.columntable
@test table.C == [:hey, :there, :sailor]

table = TableOperations.transform(ctable, Dict{Symbol, Base.Callable}(:C => Symbol)) |> Tables.columntable
@test table.C == [:hey, :there, :sailor]

table = TableOperations.transform(ctable, Dict{Int, Base.Callable}(3 => Symbol)) |> Tables.columntable
@test table.C == [:hey, :there, :sailor]

# test simple TableOperations.transforms + return types
table = ctable |> TableOperations.transform(Dict("A"=>x->x+1)) |> Tables.columntable
@test isequal(table.A, [2, missing, 4])
@test typeof(table.A) == Vector{Union{Missing, Int}}

table = ctable |> TableOperations.transform(Dict("A"=>x->coalesce(x+1, 0))) |> Tables.columntable
@test table.A == [2, 0, 4]

table = ctable |> TableOperations.transform(Dict("A"=>x->coalesce(x+1, 0.0))) |> Tables.columntable
@test table.A == [2, 0.0, 4]

table = ctable |> TableOperations.transform(Dict(2=>x->x==2.0 ? missing : x)) |> Tables.columntable
@test isequal(table.B, [1.0, missing, 3.0])
@test typeof(table.B) == Vector{Union{Float64, Missing}}

# test row sinks
# test various ways of inputting TableOperations.transform functions
table = TableOperations.transform(ctable, Dict{String, Base.Callable}("C" => Symbol)) |> Tables.rowtable
@test table[1].C == :hey

table = ctable |> TableOperations.transform(C=Symbol) |> Tables.rowtable
@test table[1].C == :hey

table = TableOperations.transform(ctable, Dict{Symbol, Base.Callable}(:C => Symbol)) |> Tables.rowtable
@test table[1].C == :hey

table = TableOperations.transform(ctable, Dict{Int, Base.Callable}(3 => Symbol)) |> Tables.rowtable
@test table[1].C == :hey

# test simple transforms + return types
table = ctable |> TableOperations.transform(Dict("A"=>x->x+1)) |> Tables.rowtable
@test isequal(map(x->x.A, table), [2, missing, 4])
@test typeof(map(x->x.A, table)) == Vector{Union{Missing, Int}}

table = ctable |> TableOperations.transform(Dict("A"=>x->coalesce(x+1, 0))) |> Tables.rowtable
@test map(x->x.A, table) == [2, 0, 4]

table = ctable |> TableOperations.transform(Dict("A"=>x->coalesce(x+1, 0.0))) |> Tables.rowtable
@test map(x->x.A, table) == [2, 0.0, 4]

table = ctable |> TableOperations.transform(Dict(2=>x->x==2.0 ? missing : x)) |> Tables.rowtable
@test isequal(map(x->x.B, table), [1.0, missing, 3.0])
@test typeof(map(x->x.B, table)) == Vector{Union{Float64, Missing}}

end

@testset "TableOperations.select" begin

# 20
x = ReallyWideTable()
sel = TableOperations.select(x, :x1, :x2)
sch = Tables.schema(sel)
@test sch.names == (:x1, :x2)
@test sch.types == (Float64, Float64)
tt = Tables.columntable(sel)
@test tt.x1 isa Vector{Float64}

sel = TableOperations.select(x, 1, 2)
sch = Tables.schema(sel)
@test sch.names == (:x1, :x2)
@test sch.types == (Float64, Float64)
tt = Tables.columntable(sel)
@test tt.x1 isa Vector{Float64}

# 117
sel = TableOperations.select(ctable)
@test Tables.istable(typeof(sel))
@test Tables.schema(sel) == Tables.Schema((), ())
@test Tables.columnaccess(typeof(sel))
@test Tables.columns(sel) === sel
@test propertynames(sel) == ()
@test isequal(Tables.getcolumn(sel, 1), [1, missing, 3])
@test isequal(Tables.getcolumn(sel, :A), [1, missing, 3])
@test Tables.columntable(sel) == NamedTuple()
@test Tables.rowtable(sel) == NamedTuple{(), Tuple{}}[]

sel = ctable |> TableOperations.select(:A)
@test Tables.istable(typeof(sel))
@test Tables.schema(sel) == Tables.Schema((:A,), (Union{Int, Missing},))
@test Tables.columnaccess(typeof(sel))
@test Tables.columns(sel) === sel
@test propertynames(sel) == (:A,)

sel = ctable |> TableOperations.select(1)
@test Tables.istable(typeof(sel))
@test Tables.schema(sel) == Tables.Schema((:A,), (Union{Int, Missing},))
@test Tables.columnaccess(typeof(sel))
@test Tables.columns(sel) === sel
@test propertynames(sel) == (:A,)

sel = TableOperations.select(rtable)
@test Tables.rowaccess(typeof(sel))
@test Tables.rows(sel) === sel
@test Tables.schema(sel) == Tables.Schema((), ())
@test Base.IteratorSize(typeof(sel)) == Base.HasShape{1}()
@test length(sel) == 3
@test Base.IteratorEltype(typeof(sel)) == Base.HasEltype()
@test eltype(sel) == TableOperations.SelectRow{NamedTuple{(:A, :B, :C),Tuple{Union{Missing, Int},Float64,String}},()}
@test Tables.columntable(sel) == NamedTuple()
@test Tables.rowtable(sel) == [NamedTuple(), NamedTuple(), NamedTuple()]
srow = first(sel)
@test propertynames(srow) == ()

sel = rtable |> TableOperations.select(:A)
@test Tables.rowaccess(typeof(sel))
@test Tables.rows(sel) === sel
@test Tables.schema(sel) == Tables.Schema((:A,), (Union{Int, Missing},))
@test Base.IteratorSize(typeof(sel)) == Base.HasShape{1}()
@test length(sel) == 3
@test Base.IteratorEltype(typeof(sel)) == Base.HasEltype()
@test eltype(sel) == TableOperations.SelectRow{NamedTuple{(:A, :B, :C),Tuple{Union{Missing, Int},Float64,String}},(:A,)}
@test isequal(Tables.columntable(sel), (A = [1, missing, 3],))
@test isequal(Tables.rowtable(sel), [(A=1,), (A=missing,), (A=3,)])
srow = first(sel)
@test propertynames(srow) == (:A,)

# Testing issue where we always select the first column values, but using the correct name.
# NOTE: We don't use rtable here because mixed types produce TypeErrors which hide the
# underlying problem.
rtable2 = [(A = 1.0, B = 2.0), (A = 2.0, B = 4.0), (A = 3.0, B = 6.0)]
sel = rtable2 |> TableOperations.select(:B)
@test Tables.rowaccess(typeof(sel))
@test Tables.rows(sel) === sel
@test Tables.schema(sel) == Tables.Schema((:B,), (Float64,))
@test Base.IteratorSize(typeof(sel)) == Base.HasShape{1}()
@test length(sel) == 3
@test Base.IteratorEltype(typeof(sel)) == Base.HasEltype()
@test eltype(sel) == TableOperations.SelectRow{NamedTuple{(:A, :B,),Tuple{Float64,Float64}},(:B,)}
@test isequal(Tables.columntable(sel), (B = [2.0, 4.0, 6.0],))
@test isequal(Tables.rowtable(sel), [(B=2.0,), (B=4.0,), (B=6.0,)])
@test isequal(Tables.columntable(sel), (B = [2.0, 4.0, 6.0],))
@test isequal(Tables.rowtable(sel), [(B=2.0,), (B=4.0,), (B=6.0,)])
srow = first(sel)
@test propertynames(srow) == (:B,)
@test srow.B == 2.0 # What we expect

sel = rtable |> TableOperations.select(1)
@test Tables.rowaccess(typeof(sel))
@test Tables.rows(sel) === sel
@test Tables.schema(sel) == Tables.Schema((:A,), (Union{Int, Missing},))
@test Base.IteratorSize(typeof(sel)) == Base.HasShape{1}()
@test length(sel) == 3
@test Base.IteratorEltype(typeof(sel)) == Base.HasEltype()
@test eltype(sel) == TableOperations.SelectRow{NamedTuple{(:A, :B, :C),Tuple{Union{Missing, Int},Float64,String}},(1,)}
@test isequal(Tables.columntable(sel), (A = [1, missing, 3],))
@test isequal(Tables.rowtable(sel), [(A=1,), (A=missing,), (A=3,)])
srow = first(sel)
@test propertynames(srow) == (:A,)
@test Tables.getcolumn(srow, 1) == 1
@test Tables.getcolumn(srow, :A) == 1

table = ctable |> TableOperations.select(:A) |> Tables.columntable
@test length(table) == 1
@test isequal(table.A, [1, missing, 3])

table = ctable |> TableOperations.select(1) |> Tables.columntable
@test length(table) == 1
@test isequal(table.A, [1, missing, 3])

table = ctable |> TableOperations.select("A") |> Tables.columntable
@test length(table) == 1
@test isequal(table.A, [1, missing, 3])

# column re-ordering
table = ctable |> TableOperations.select(:A, :C) |> Tables.columntable
@test length(table) == 2
@test isequal(table.A, [1, missing, 3])
@test isequal(table[2], ["hey", "there", "sailor"])

table = ctable |> TableOperations.select(1, 3) |> Tables.columntable
@test length(table) == 2
@test isequal(table.A, [1, missing, 3])
@test isequal(table[2], ["hey", "there", "sailor"])

table = ctable |> TableOperations.select(:C, :A) |> Tables.columntable
@test isequal(ctable.A, table.A)
@test isequal(ctable[1], table[2])

table = ctable |> TableOperations.select(3, 1) |> Tables.columntable
@test isequal(ctable.A, table.A)
@test isequal(ctable[1], table[2])

# row sink
table = ctable |> TableOperations.select(:A) |> Tables.rowtable
@test length(table[1]) == 1
@test isequal(map(x->x.A, table), [1, missing, 3])

table = ctable |> TableOperations.select(1) |> Tables.rowtable
@test length(table[1]) == 1
@test isequal(map(x->x.A, table), [1, missing, 3])

table = ctable |> TableOperations.select("A") |> Tables.rowtable
@test length(table[1]) == 1
@test isequal(map(x->x.A, table), [1, missing, 3])

# column re-ordering
table = ctable |> TableOperations.select(:A, :C) |> Tables.rowtable
@test length(table[1]) == 2
@test isequal(map(x->x.A, table), [1, missing, 3])
@test isequal(map(x->x[2], table), ["hey", "there", "sailor"])

table = ctable |> TableOperations.select(1, 3) |> Tables.rowtable
@test length(table[1]) == 2
@test isequal(map(x->x.A, table), [1, missing, 3])
@test isequal(map(x->x[2], table), ["hey", "there", "sailor"])

table = ctable |> TableOperations.select(:C, :A) |> Tables.rowtable
@test isequal(ctable.A, map(x->x.A, table))
@test isequal(ctable[1], map(x->x[2], table))

table = ctable |> TableOperations.select(3, 1) |> Tables.rowtable
@test isequal(ctable.A, map(x->x.A, table))
@test isequal(ctable[1], map(x->x[2], table))

end

@testset "TableOperations.filter" begin

f = TableOperations.filter(x->x.B == 2.0, ctable)
@test Tables.istable(f)
@test Tables.rowaccess(f)
@test Tables.rows(f) === f
@test Tables.schema(f) == Tables.schema(f)
@test Base.IteratorSize(typeof(f)) == Base.SizeUnknown()
@test Base.IteratorEltype(typeof(f)) == Base.HasEltype()
@test eltype(f) == eltype(Tables.rows(ctable))
@test isequal(Tables.columntable(f), Tables.columntable(ctable |> TableOperations.filter(x->x.B == 2.0)))
@test length((TableOperations.filter(x->x.B == 2.0, ctable) |> Tables.columntable).B) == 1
@test length((TableOperations.filter(x->x.B == 2.0, rtable) |> Tables.columntable).B) == 1
@test length(TableOperations.filter(x->x.B == 2.0, ctable) |> Tables.rowtable) == 1
@test length(TableOperations.filter(x->x.B == 2.0, rtable) |> Tables.rowtable) == 1

end

@testset "TableOperations.map" begin

m = TableOperations.map(x->(A=x.A, C=x.C, B=x.B * 2), ctable)
@test Tables.istable(m)
@test Tables.rowaccess(m)
@test Tables.rows(m) === m
@test Tables.schema(m) === nothing
@test Base.IteratorSize(typeof(m)) == Base.HasLength()
@test Base.IteratorEltype(typeof(m)) == Base.EltypeUnknown()
@test isequal(Tables.columntable(m), Tables.columntable(ctable |> TableOperations.map(x->(A=x.A, C=x.C, B=x.B * 2))))
@test (TableOperations.map(x->(A=x.A, C=x.C, B=x.B * 2), ctable) |> Tables.columntable).B == [2.0, 4.0, 6.0]
@test (TableOperations.map(x->(A=x.A, C=x.C, B=x.B * 2), rtable) |> Tables.columntable).B == [2.0, 4.0, 6.0]
@test length(TableOperations.map(x->(A=x.A, C=x.C, B=x.B * 2), ctable) |> Tables.rowtable) == 3
@test length(TableOperations.map(x->(A=x.A, C=x.C, B=x.B * 2), rtable) |> Tables.rowtable) == 3

end

@testset "TableOperations.joinpartitions" begin

p = Tables.partitioner((ctable, ctable))
j = TableOperations.joinpartitions(p)
@test Tables.istable(j)
@test Tables.columnaccess(j)
@test Tables.schema(j) === Tables.schema(ctable)
@test Tables.columnnames(j) == Tables.columnnames(ctable)
@test isequal(Tables.getcolumn(j, 1), vcat(Tables.getcolumn(ctable, 1), Tables.getcolumn(ctable, 1)))
@test isequal(Tables.getcolumn(j, :A), vcat(Tables.getcolumn(ctable, :A), Tables.getcolumn(ctable, :A)))

# Test joinpartitions with promotion
t1 = (A=[1, 2, 3], B=[1, 2, 3], C=["hey", "there", "sailor"])
t2 = (A=[1, missing, 3], B=[1.0, 2.0, 3.0], C=["trim", "the", "sail"])
p = Tables.partitioner((t1, t2))
# Throws a method error trying to convert `missing` to `Int64`
@test_throws MethodError TableOperations.joinpartitions(p)
j = TableOperations.joinpartitions(p; promote=true)
@test Tables.istable(j)
@test Tables.columnaccess(j)
@test Tables.schema(j) !== Tables.schema(t1)
@test Tables.schema(j) === Tables.schema(t2)
@test Tables.columnnames(j) == Tables.columnnames(t1) == Tables.columnnames(t2)
@test isequal(Tables.getcolumn(j, 1), vcat(Tables.getcolumn(t1, 1), Tables.getcolumn(t2, 1)))
@test isequal(Tables.getcolumn(j, :A), vcat(Tables.getcolumn(t1, :A), Tables.getcolumn(t2, :A)))
end

@testset "TableOperations.makepartitions" begin

# columns
@test_throws ArgumentError TableOperations.makepartitions(ctable, 0)
parts = collect.(collect(Tables.partitions(TableOperations.makepartitions(ctable, 2))))
@test length(parts[1]) == 2
@test length(parts[2]) == 1
@test parts[2][1].A == 3

# rows
parts = collect(Tables.partitions(TableOperations.makepartitions(rtable, 2)))
@test length(parts[1]) == 2
@test length(parts[2]) == 1
@test parts[2][1].A == 3

# forward-only row iterator
parts = collect(Tables.partitions(TableOperations.makepartitions(rtable2, 3)))
@test length(parts) == 4
@test length(parts[1]) == 3
@test length(parts[end]) == 1
@test parts[end][1].a == 20

end

@testset "TableOperations.narrowtypes" begin

ctable_type_any = (A=Any[1, missing, 3], B=Any[1.0, 2.0, 3.0], C=Any["hey", "there", "sailor"])

nt = TableOperations.narrowtypes(ctable_type_any)
@test Tables.istable(nt)
@test Tables.columnaccess(nt)
@test Tables.schema(nt) == Tables.schema(ctable)
@test Tables.columnnames(nt) == Tables.columnnames(ctable)

end

@testset "TableOperations.dropmissing" begin

table = ctable |> TableOperations.dropmissing() |> Tables.columntable
@test isequal(table, Tables.columntable(TableOperations.dropmissing(ctable)))
@test length(table |> Tables.columntable) == 3
@test length(table |> Tables.rowtable) == 2

end
