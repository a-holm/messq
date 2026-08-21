module github.com/a-holm/messq

go 1.25.0

toolchain go1.26.5

require example.invalid/forbidden v0.0.0

replace example.invalid/forbidden => ./unusedmod
