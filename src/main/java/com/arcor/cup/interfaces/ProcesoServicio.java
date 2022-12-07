package com.arcor.cup.interfaces;

import java.sql.Connection;

import java.util.List;

import com.arcor.libs.procesoservicios.cup.estructuras.TablaProcesoServicio;

public interface ProcesoServicio {
	
	void proceso();
	
	void internalService(Connection con);
	
	void procesarServicios(List<TablaProcesoServicio> lista, Connection con);

}
