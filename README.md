# php-hdfs-client

It is a library for accessing hdfs from php.
You can install using Composer.
We use webhdfs as communication protocol.

## Description

**None**

## Demo

```php
<?php
// ...

$hdfsClient = new HdfsClient('your_hdfs_namenode');

$hdfsFilePath = '/user/hive/warehouse/test.db/foo_table';

$hdfsFileContents = """\
1,2,3
4,5,6""";

$status = $hdfsClient->putFileToRemote($hdfsFilePath, $hdfsFileContents);

// Error Case!
if ($status === false || !empty(json_decode($status, true)['RemoteException']['exception'])) {
    throw new Exception('Hdfs fileput error');
}
```

## Requirements

* [Composer](https://getcomposer.org/)

## Dependencies

None.

## Installation

```bash
$ composer require "genzouw/php-hdfs-client:dev-master"
```

## Relase Note

| date       | version | note           |
| ---        | ---     | ---            |
| 2018-09-07 | 0.1     | first release. |

This software is released under the MIT License, see LICENSE.

## Author Information

[genzouw](https://genzouw.com)

* Twitter   : @genzouw ( https://twitter.com/genzouw )
* Facebook  : genzouw ( https://www.facebook.com/genzouw )
* LinkedIn  : genzouw ( https://www.linkedin.com/in/genzouw/ )
* Gmail     : genzouw@gmail.com
