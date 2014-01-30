
# How to run

-   Copy model.config.sample to /opt/bigtable-ui/config/model.config and change appropriately. Alternatively you can set the location in the web.xml of the config file.

-   Run `mvn jetty:run`

    This will start the server at [http://localhost:8080](http://localhost:8080).  TO run on a
    different port, use `mvn -Djetty.port=PORT jetty:run`.
