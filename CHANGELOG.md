## [Unreleased]

### Added

- Add `Quoting#quoted_binary`. This lets the adapter write binary columns without bind parameters.

### Fixed

- Fix quoted timestamps. They now keep their sub-second precision. They now honour
  `ActiveRecord.default_timezone`. This fix applies with `prepared_statements: false`, and always
  in Quack mode.
- Fix quoted binary values. They no longer fail with `Parser Error: unterminated quoted string`.

## [0.1.0] - 2025-06-18

- Initial release
