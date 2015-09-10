GeoExport
----

## Running

### Dependencies
* You need to have `zookeeper` running.
* You need to have `soda-fountain` running.
* `soda-fountain` must be registered in `zookeeper` and responding to requests

### From the console
`sbt run`

this will start the service on the default port (7777)

## Testing
You shouldn't need anything fancy to test. Just run `sbt test`

## Using

### Endpoints

##### /version
```
GET /version
````
get the version of the service


##### /export
```
GET /export/{uid_0},{uid_1},...,{uid_n}.{format}
```
where
```
uid:    a dataset identifier (four by four)
format: an export format (currently only one of {shp, kml, kmz})
```

export the comma separated list of datasets (identified by their uid)
as a merged file in the format specified. note that this list of datasets
doesn't need to be related in any way, they just need to have one shape column.



