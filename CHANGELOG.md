# Changelog

## [0.3.0] - 2020-10-12

### Added
- include celery 5 in dependency

## [0.2.0] - 2019-05-24

### Added
- This changelog
- Support for custom storage backend implementations
- Configurable backend URL for default or custom storage backend (to e.g. use a different redis server)
- Configurable key prefix for locks
- `lock\_expiry` option
- `raise\_on\_duplicate` option
- `unique\_on` option
