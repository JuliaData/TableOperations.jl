# Adding transforms to CSV with header but no data returns empty frame as expected
# (previously the lack of a ::String dispatch in the transform function caused an error)
transforms = Dict{Int, Function}(2 => x::Integer -> "b$x")
df1 = CSV.File(IOBuffer("a,b,c\n1,2,3\n4,5,6"); allowmissing=:none, transforms=transforms)
df2 = CSV.File(IOBuffer("a,b,c\n1,b2,3\n4,b5,6"); allowmissing=:none)
@test size(Data.schema(df1)) == (2, 3)
@test size(Data.schema(df2)) == (2, 3)
@test df1 == df2
df3 = CSV.File(IOBuffer("a,b,c"); allowmissing=:none, transforms=transforms)
df4 = CSV.File(IOBuffer("a,b,c"); allowmissing=:none)
@test size(Data.schema(df3)) == (0, 3)
@test size(Data.schema(df4)) == (0, 3)
@test df3 == df4

let fn = tempname()
    CSV.File(IOBuffer("a,b,c\n1,2,3\n4,5,6"), CSV.Sink(fn); allowmissing=:none, transforms=transforms)
    @test String(read(fn)) == "a,b,c\n1,b2,3\n4,b5,6\n"
    @try rm(fn)
end

let fn = tempname()
    CSV.File(IOBuffer("a,b,c"), CSV.Sink(fn); allowmissing=:none, transforms=transforms)
    @test String(read(fn)) == "a,b,c\n"
    @try rm(fn)
end

source = IOBuffer("col1,col2,col3") # empty dataset
f = CSV.File(source; transforms=Dict(2 => floor))
@test size(Data.schema(df)) == (0, 3)
@test Data.types(Data.schema(df)) == (Any, Any, Any)
