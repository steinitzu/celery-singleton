# Changelog

### Added
- Support passing an optional custom [`json.JSONEncoder`] to `util.generate_lock()` via `singleton_json_encoder_class`.
  Useful for task arguments with objects marshalable to the same string representation, e.g. passing [`uuid.UUID`] to  [`str()`].

  PR [#44](https://github.com/steinitzu/celery-singleton/pull/44) by [Tony Narlock](https://github.com/tony) in regards to [#42](https://github.com/steinitzu/celery-singleton/issues/42) and [#36](https://github.com/steinitzu/celery-singleton/issues/36).

[`json.JSONEncoder`]: https://docs.python.org/3/library/json.html#json.JSONEncoder
[`str()`]: https://docs.python.org/3/library/stdtypes.html#str
[`uuid.UUID`]: https://docs.python.org/3/library/uuid.html#uuid.UUID

## [0.3.1] - 2021-01-14

### Changed
- Correct signal usage in README [#30](https://github.com/steinitzu/celery-singleton/pull/30) by [@reorx](https://github.com/reorx)
- Fix wrong repository and homepage URL in pyproject.toml (thanks [@utapyngo](https://github.com/utapyngo) for pointing it out)

## [0.3.0] - 2020-10-12

### Added
- Support Celery 5. PR [#30](https://github.com/steinitzu/celery-singleton/pull/30) by [@wangsha](https://github.com/wangsha)

### Removed
- Remove python 3.5 support (dropped by Celery 5)

## [0.2.0] - 2019-05-24

### Added
- This changelog
- Support for custom storage backend implementations
- Configurable backend URL for default or custom storage backend (to e.g. use a different redis server)
- Configurable key prefix for locks
- `lock\_expiry` option
- `raise\_on\_duplicate` option
- `unique\_on` option
