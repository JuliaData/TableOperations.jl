# TableOperations

*Common table operations on Tables.jl compatible sources*

[![][travis-img]][travis-url] [![][codecov-img]][codecov-url]

## Documentation

### `TableOperations.select`
The `TableOperations.select` function allows specifying a custom subset and order of columns from a Tables.jl source, like:
```julia
ctable = (A=[1, missing, 3], B=[1.0, 2.0, 3.0], C=["hey", "there", "sailor"])

table_subset = ctable |> TableOperations.select(:C, :A) |> Tables.columntable
```
This "selects" the `C` and `A` columns from the original table, and re-orders them with `C` first. The column names can be provided as `String`s, `Symbol`s, or `Integer`s.

### `TableOperations.transform`
The `TableOperations.transform` function allows specifying a "transform" function per column that will be applied per element. This is handy
when a simple transformation is needed for a specific column (or columns). Note that this doesn't allow the creation of new columns,
but only applies the transform function to the specified column, and thus, replacing the original column. Usage is like:
```julia
ctable = (A=[1, missing, 3], B=[1.0, 2.0, 3.0], C=["hey", "there", "sailor"])

table = ctable |> TableOperations.transform(C=x->Symbol(x)) |> Tables.columntable
```
Here, we're providing the transform function `x->Symbol(x)`, which turns an argument into a `Symbol`, and saying we should apply it to the `C` column.
Multiple tranfrom functions can be provided for multiple columns and the column to transform function can also be provided in `Dict`s that
map column names as `String`s, `Symbol`s, or even `Int`s (referring to the column index).

### `TableOperations.filter`

The `TableOperations.filter` function allows applying a "filter" function to each row in the input table source, keeping rows for which `f(row)` is `true`.
Usage is like:
```julia
ctable = (A=[1, missing, 3], B=[1.0, 2.0, 3.0], C=["hey", "there", "sailor"])

table = ctable |> TableOperations.filter(x->Tables.getcolumn(x, :B) > 2.0) |> Tables.columntable
```

### `TableOperations.map`
The `TableOperations.map` function allows applying a "mapping" function to each row in the input table source; the function `f` should take and
return a Tables.jl `Row` compatible object. Usage is like:
```julia
ctable = (A=[1, missing, 3], B=[1.0, 2.0, 3.0], C=["hey", "there", "sailor"])

table = ctable |> TableOperations.map(x->(A=Tables.getcolumn(x, :A), C=Tables.getcolumn(x, :C), B=Tables.getcolumn(x, :B) * 2)) |> Tables.columntable
```

## Contributing and Questions

Contributions are very welcome, as are feature requests and suggestions. Please open an
[issue][issues-url] if you encounter any problems or would just like to ask a question.

[travis-img]: https://travis-ci.org/JuliaData/TableOperations.jl.svg?branch=master
[travis-url]: https://travis-ci.org/JuliaData/TableOperations.jl

[codecov-img]: https://codecov.io/gh/JuliaData/TableOperations.jl/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/JuliaData/TableOperations.jl

[issues-url]: https://github.com/JuliaData/TableOperations.jl/issues
