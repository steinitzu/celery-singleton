# Changelog

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
