# ms-digitalfix-report (puerto 8084)

[![CI](https://github.com/DigitalFix-Grupo12/ms-digitalfix-report/actions/workflows/ci.yml/badge.svg)](https://github.com/DigitalFix-Grupo12/ms-digitalfix-report/actions/workflows/ci.yml)

KPIs calculados en linea a partir de las ordenes reales de `ms-digitalfix-workorders`
(ordenes por hora, tiempo promedio de resolucion, ordenes activas, conteo por estado y serie temporal).

| Metodo | Ruta | Descripcion |
|---|---|---|
| GET | /api/report/kpis?range=last24h\|last7d | KPIs; 503 si workorders no responde |

Variable: `WORKORDERS_URL` (default `http://localhost:8082`).
## Arquitectura

```
Angular (MSAL) -> AWS API Gateway -> ms-digitalfix-bff :8080 (valida JWT Entra ID + rol)
                                         |-> ms-digitalfix-workorders :8082 -> audit
                                         |-> ms-digitalfix-catalog    :8083
                                         |-> ms-digitalfix-report     :8084 -> workorders
                                         |-> ms-digitalfix-audit      :8085
```

Los microservicios de dominio **no validan JWT**: solo escuchan en la red interna
del host (el Security Group expone unicamente el 8080 del BFF). El BFF propaga la
identidad del usuario en el header `X-User-Name`.

## Ejecutar

```
mvn clean package
java -jar target/ms-digitalfix-report-0.0.1-SNAPSHOT.jar
```

Health: `GET /actuator/health`. Base de datos: H2 en memoria (se reinicia con el servicio).