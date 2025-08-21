import os
import uvicorn

if __name__ == "__main__":
    hostip = "0.0.0.0"
    # hostip = constants.SERVER_IP
    # For Local
    # uvicorn.run("app:app",
    #             host=hostip, port=8000, reload=True, workers=5)

    # For Server
    uvicorn.run("app:app",
                host=hostip, port=80, reload=True, workers=5,
                #ssl_keyfile="/opt/bitnami/apache/conf/bitnami/certs/new_cert/server.key",
                #ssl_certfile="/opt/bitnami/apache/conf/bitnami/certs/new_cert/server.crt",
                #ssl_ca_certs="/opt/bitnami/apache/conf/bitnami/certs/new_cert/ca_bundle.crt"
                )
