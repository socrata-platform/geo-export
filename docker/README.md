# GeoExport Docker Config #
To build the image, run:
    `docker build -t geo-export .`

Or, if you want to replace old versions:
    `docker build --rm -t geo-export .`

## Required Environment Variables ##
* `ZOOKEEPER_ENSEMBLE` - A list of hostnames and ports of zookeeper instances. eg: ["10.0.0.1:2181", "10.0.0.2:2818"]

## Optional Runtime Variables ##
See the [DockerFile](Dockerfile) for defaults.

* `JAVA_XMX`                - Sets the JVM heap size.
* `MIN_THREADS`             - Sets the minimum number of server threads.
* `MAX_THREADS`             - Sets the maximum number of server threads.

##TODO: figure out reasonable values for xmx and min/max and such