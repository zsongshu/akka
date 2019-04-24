# Migration Guide 2.5.x to 2.6.x

## Removed module: akka-contrib

The akka-contrib module was deprecated in 2.5 and has been removed in 2.6.
To migrate, take the components you are using from [Akka 2.5](https://github.com/akka/akka/tree/release-2.5/akka-contrib)
and include them in your own project or library under your own package name.

## Scala 2.11 no longer supported

If you are still using Scala 2.11 then you must upgrade to 2.12 or 2.13
