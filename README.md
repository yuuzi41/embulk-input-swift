#  OpenStack Swift Object Storage file input plugin for Embulk

[Embulk](http://www.embulk.org/) file input plugin stores to [OpenStack](https://www.openstack.org/) [Object Storage(Swift)](http://swift.openstack.org/)
This plugin also may be usable for Swift compatible cloud storages, such as [Rackspace](https://www.rackspace.com/), and more.

This plugin uses Java OpenStack Storage(JOSS) library.

## Overview

* **Plugin type**: file input
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

### Common

- **container**: Container name (string, required)
- **path_prefix**: prefix of target objects (string, required)
- **path_match_pattern**: regexp to match file paths. If a file path doesn't match with this pattern, the file will be skipped (regexp string, optional)
- **total_file_count_limit**: maximum number of files to read (integer, optional)
- **min_task_size (experimental)**: minimum bytesize of a task. If this is larger than 0, one task includes multiple input files up until it becomes the bytesize in total. This is useful if too many number of tasks impacts performance of output or executor plugins badly. (integer, optional)

### With authentication

if you want to use Swift with authentication (keystone, tempauth or basic)

- **auth_type**: Authentication type. you can choose "tempauth", "keystone", or "basic". (string, required)
- **username**: username for accessing the object storage (string, required)
- **password**: password for accessing the object storage (string, required)
- **auth_url**: authentication url for accessing the object storage (string, required)
- **tenant_id**: Tenant Id for keystone authentication (string, optional)
- **tenant_name**: Tenant name for keystone authentication (string, optional)

### Without authentication

if you want to use Swift without authentication,

- **auth_type**: Authentication type. you must choose "noauth" for swift without authentication. (string, required)
- **endpoint_url**: The Endpoint URL for Swift, e.g. "http://swift-proxy:8080/v1" (string, required)
- **account**: Account name for input. (string, required)

## Example

```yaml
in:
  type: swift
  auth_type: tempauth
  username: test:tester
  password: testing
  auth_url: http://localhost:8080/auth/v1.0
  container: embulk_input
  path_prefix: data
  parser:
    type: csv
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
