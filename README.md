# Azure Synapse DB writer


Writes data to Azure Synapse Database

# Usage

## Configuration

- `db` - object (required)
  - `host` - string (required)
  - `port` - int (optional, default value `1433`)
  - `user` - string (required)
  - `#password` - string (required)
  - `database` - string (required)
  - `schema` - string (optional, default value `dbo`)
- `tableId` - string (required)
- `dbName` - string (required)
- `absCredentialsType` - enum (optional): `sas` (default), `managed_identity`
- `incremental` - boolean (optional, default value `false`)
- `export` - boolean (optional, default value `true`)
- `primaryKey` - array of string (optional)
- `items` - array of object (optional)
    - `name` - string (required)
    - `dbName` - string (required)
    - `type` - string (required)
    - `size` - string (optional)
    - `nullable` - string (optional)
    - `default` - string (required)

## Global config

Image or Stack Parameters can be used to set default value `absCredentialsType` to the writer. 

The global configuration is stored under key `image_parameters.global_config.absCredentialsType` and has LOWER priority than the value in the `parameters`.

Example for default `absCredentialsType` value:
```json
{
  "parameters": {
    "data_dir": "data/dir",
    "tableId": "test-table-id",
    "dbName": "db-table-name",
    "db": {
      "host": "synapse-server",
      "port": 1433,
      "user": "synapse-user",
      "#password": "synapse-secret-password",
      "database": "synapse-database"
    }
  },
  "image_parameters": {
    "global_config": {
      "absCredentialsType": "managed_identity"
    }
  }
}
```

## Examples

Test connection:
```json
{
  "action": "testConnection",
  "parameters": {
    "db": {
      "host": "synapse-server",
      "port": 1433,
      "user": "synapse-user",
      "#password": "synapse-secret-password",
      "database": "synapse-database"
    }
  }
}
```

Simple write data with PK:
```json
{
  "parameters": {
    "db": {
      "host": "synapse-server",
      "port": 1433,
      "user": "synapse-user",
      "#password": "synapse-secret-password",
      "database": "synapse-database"
    },
    "tableId": "simple",
    "dbName": "simple",
    "primaryKey": [
      "id"
    ],
    "items": [
      {
        "name": "id",
        "dbName": "id",
        "type": "int",
        "size": null,
        "nullable": false,
        "default": null
      },
      {
        "name": "name",
        "dbName": "name",
        "type": "varchar",
        "size": 255,
        "nullable": false,
        "default": null
      }
    ]
  }
}
```

Incremental write:
```json
{
  "parameters": {
    "db": {
      "host": "synapse-server",
      "port": 1433,
      "user": "synapse-user",
      "#password": "synapse-secret-password",
      "database": "synapse-database"
    },
    "incremental": true,
    "tableId": "incremental",
    "dbName": "incremental",
    "primaryKey": [
      "id"
    ],
    "items": [
      {
        "name": "id",
        "dbName": "id",
        "type": "int",
        "size": null,
        "nullable": null,
        "default": null
      },
      {
        "name": "name",
        "dbName": "name",
        "type": "varchar",
        "size": 255,
        "nullable": false,
        "default": null
      }
    ]
  }
}
```

# Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/db-writer-synapse
cd db-writer-synapse
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
 
# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 

## License

MIT licensed, see [LICENSE](./LICENSE) file.
