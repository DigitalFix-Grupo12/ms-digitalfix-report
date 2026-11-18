package cl.duoc.digitalfix.report.service;

import cl.duoc.digitalfix.report.client.WorkOrdersClient.WorkOrderView;

import java.util.List;

/** Origen de las ordenes con que se calculan los KPIs (la proyeccion alimentada por Kafka). */
@FunctionalInterface
public interface OrderViews {

    List<WorkOrderView> findAll();
}
