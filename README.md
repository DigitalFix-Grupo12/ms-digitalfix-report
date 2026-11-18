# ms-digitalfix-report (puerto 8084)

[![CI](https://github.com/DigitalFix-Grupo12/ms-digitalfix-report/actions/workflows/ci.yml/badge.svg)](https://github.com/DigitalFix-Grupo12/ms-digitalfix-report/actions/workflows/ci.yml)

KPIs en tiempo (cuasi) real de la red: ordenes por hora, tiempo promedio de resolucion, ordenes activas,
conteo por estado y serie temporal. Se calculan desde una **proyeccion local** que mantienen los eventos
de Kafka (`workorders.events`): reporteria no consulta ni bloquea a `ms-digitalfix-workorders`.

| Metodo | Ruta | Descripcion |
|---|---|---|
| GET | /api/report/kpis?range=last24h\|last7d | KPIs desde la proyeccion |
| GET | /api/report/live | Panel en vivo: KPIs + ultimos eventos consumidos + ordenes con SLA en riesgo + consumo por particion |

Variables: `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092,localhost:9093,localhost:9094`),
`WORKORDERS_URL` (solo snapshot inicial), `REPORT_SLA_MINUTES` (default 30), `REPORT_SNAPSHOT` (default `true`).

## Streaming (consumidor Kafka)

```
workorders.events (3 particiones, RF 3)
   |-- grupo report-kpis  (3 hilos, 1 por particion) -> WorkOrderEventsListener -> ProjectionService -> order_projection
   |-- grupo audit-timeline (ms-digitalfix-audit)     -> fan-out: cada grupo lee todo a su ritmo
```

- **Proyeccion (read model)**: `order_projection` (estado, cliente, tecnico, creada, cerrada, ultimo cambio) +
  `processed_events` (idempotencia por `eventId`). `CLOSED` fija `closedAt` = base del tiempo de resolucion.
- **Consumer group `report-kpis`**: `concurrency=3` (= particiones). Con la BD compartida del perfil `cloud`
  se pueden levantar mas instancias del mismo grupo y Kafka les reparte las particiones
  (`CooperativeStickyAssignor`, rebalanceo incremental).
- **Errores**: `ErrorHandlingDeserializer` (JSON ilegible), Bean Validation del payload (`InvalidEventException`,
  sin reintentos), fallas transitorias con 3 reintentos y backoff exponencial; luego
  `workorders.events.report-kpis.DLT` (misma particion, headers `kafka_dlt-*`). Logs con `eventId`, `traceId`,
  topico, particion y offset.
- **Proceso derivado**: `SlaMonitor` revisa la proyeccion cada 30 s y marca **SLA en riesgo** las ordenes activas
  sin cambios por mas de `REPORT_SLA_MINUTES`; el siguiente evento de esa orden limpia la marca.
- **Snapshot inicial**: al arrancar copia una vez las ordenes que existian antes de los eventos (datos de demo);
  desde ahi solo Kafka actualiza la proyeccion.
## Arquitectura

```
Angular (MSAL) -> AWS API Gateway -> ms-digitalfix-bff :8080 (valida JWT Entra ID + rol)
                                         |-> ms-digitalfix-workorders :8082 -> Kafka workorders.events
                                         |-> ms-digitalfix-catalog    :8083
                                         |-> ms-digitalfix-report     :8084 <- Kafka (grupo report-kpis)
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