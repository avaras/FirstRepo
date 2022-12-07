package com.arcor.cup.service;

import com.arcor.libs.procesoservicios.cup.estructuras.TablaProcesoServicio;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
//import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

// import java.util.ArrayList;
// import java.util.Date;
import java.util.List;

// import javax.jws.Oneway;
// import javax.jws.WebMethod;
// import javax.jws.WebParam;
// import javax.jws.WebService;

import org.springframework.beans.factory.annotation.Autowired;

import com.arcor.cup.interfaces.ProcesoServicio;
import com.arcor.libs.parametrosutils.cup.ParametrosUtils;


import org.apache.log4j.Logger;

//import cup.utils.ParametrosUtils;

import com.arcor.libs.procesoservicios.cup.comunes.ClassFactory;
import com.arcor.libs.procesoservicios.cup.comunes.Mensajes;
import com.arcor.libs.procesoservicios.cup.comunes.NombresServicios;

// import com.arcor.libs.procesoservicios.cup.servicios.CupSaldoCtaCrte;

import com.arcor.libs.procesoservicios.cup.template.LlamadaServicio;

public class ProcesoJdeAltas implements ProcesoServicio {

    @Autowired
    DataSource dataSource;

    private static final Logger log = Logger.getLogger(ProcesoJdeAltas.class.getName());

    /*
     * CAMPO PROCESO_SLEEP EN DB Cuanto duerme en segundos el proceso entre cada
     * lote de servicios a procesar. Este valor tiene valiez si no esta seteado el
     * campo PROCESO_SLEEP en la BD
     */
    public static int SLEEP_PROCESO = 5;

    /*
     * CAMPO PROCESO_MAX_POR_LLAMADA EN DB Cantidad de servicios a procesar en una
     * sola llamada a los webservices que admiten multiples servicios en una unica
     * llamada
     */
    public static int MAX_POR_LLAMADA = 5;

    /*
     * CAMPO PROCESO_MAX_REINTENTOS EN DB Cantidad de maxima de reintentos a
     * realizar por servicio
     */
    public static int MAX_REINTENTOS = 50;

    /*
     * CAMPO PROCESO_MAX_INTERVALO_ENTRE_LLAMDAS EN DB Cantidad de maxima de horas a
     * esperar al reintentar procesar un servicio
     */
    public static int MAX_INTERVALO_ENTRE_LLAMADAS = 240;

    @Override
    public void proceso() {
    	
        try {

            log.info("Get Connection from dataSource");
            Connection con = dataSource.getConnection();

            // NO VA
            // synchronized (monitor) {
            // ADFContext currentADFContext = null;
            // try {
            // currentADFContext =
            // ADFContext.initADFContext(null, null, null, null);
            // log.warning("* Dentro de zona sincronizada *");
            // internalService(minutos);
            // } finally {
            // ADFContext.resetADFContext(currentADFContext);
            // }
            // }

            internalService(con);

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            log.error(e);
        }

    }

    public void internalService(Connection con) {

//        Long start = System.currentTimeMillis();
        Mensajes retorno = Mensajes.PROCESO_OK;
        List<TablaProcesoServicio> serviciosJdeAltas;

        log.info("------------ Inicio Proceso de Servicios ------------");

        log.info("------------  Proceso de Servicios : Obteniendo Parametros ------------");

        // ----------------------------
        // Obtiene parametro sleep
        // ----------------------------

        String sleepTmp = ParametrosUtils.obtenerParametro(con, "PROCESO_SLEEP");

        ProcesoJdeAltas.SLEEP_PROCESO = sleepTmp != null ? Integer.valueOf(sleepTmp)
                : ProcesoJdeAltas.SLEEP_PROCESO;
        log.info("Sleep de proceso -> " + ProcesoJdeAltas.SLEEP_PROCESO + " segundos");

        // ----------------------------
        // Cantidad maxima servicios a procesar por llamada
        // ----------------------------
        String maxPorLlamadaTmp = ParametrosUtils.obtenerParametro(con, "PROCESO_MAX_POR_LLAMADA");

        ProcesoJdeAltas.MAX_POR_LLAMADA = maxPorLlamadaTmp != null ? Integer.valueOf(maxPorLlamadaTmp)
                : ProcesoJdeAltas.MAX_POR_LLAMADA;
        log.info("Cantidad maxima a enviar por llamada -> " + maxPorLlamadaTmp);

        // ----------------------------
        // Cantidad maxima de intentos por cada webservice
        // ----------------------------
        String maxIntentosTmp = ParametrosUtils.obtenerParametro(con, "PROCESO_MAX_INTENTOS");

        ProcesoJdeAltas.MAX_REINTENTOS = maxIntentosTmp != null ? Integer.valueOf(maxIntentosTmp)
                : ProcesoJdeAltas.MAX_REINTENTOS;
        log.info("Cantidad maxima a procesar por servicio -> " + maxIntentosTmp);

        // ----------------------------
        // Cantidad maxima de intentos por cada webservice
        // ----------------------------
        String maxIntervalo = ParametrosUtils.obtenerParametro(con, "PROCESO_MAX_INTERVALO_ENTRE_LLAMADAS");
        ProcesoJdeAltas.MAX_INTERVALO_ENTRE_LLAMADAS = maxIntervalo != null ? Integer.valueOf(maxIntervalo)
                : ProcesoJdeAltas.MAX_INTERVALO_ENTRE_LLAMADAS;
        log.info("Cantidad maxima de intervalo entre llamadas -> " + maxIntervalo);

        // ----------------------------
        // Set variables de auditoria y control
        // ----------------------------
        int loop = 0;
        
       
        do {
            try {
				if (con != null && con.isValid(1)) {
					log.info("Closing Connection");
					con.close();
				}

				log.info("-----Get New Connection from dataSource------");
				con = dataSource.getConnection();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				log.warn(e1.getMessage());
			}


            loop++;

            // Cantidad maxima de intentos

            /*
             * Obtiene Lista de eventos a procesar
             */

            serviciosJdeAltas = TablaProcesoServicio
                    .getServiciosAProcesar(con,NombresServicios.WS_AltaModif_Metadata.toString(),
                            NombresServicios.WS_AltaImagen.toString());
                    // TODO no se cual seria el de mi modulo
            /*
             * Informo cantidad obtenidos
             */
            log.info("------------ Eventoss #" + serviciosJdeAltas.size() + " ------------");
            log.info("------------ Loop #" + loop + " ------------");

            /*
             * Procesa servicios
             */
            procesarServicios(serviciosJdeAltas, con);

            /*
             * En caso de que se vaya a repetir el procesamiento, espera SLEEP_PROCESO
             * segundos con el fin de no sobrecargar el cpu
             */
            
//            log.info("Tiempo empleado: " + (System.currentTimeMillis() - start) + " milisegundos");
            log.info("Retorno proceso " + retorno.getMensaje() + " (" + retorno.getCodigo() + ")");
            log.info("------------ FIN PROCESO DE SERVICIOS ------------");
            
            
            try {
                Thread.sleep(SLEEP_PROCESO * 1000);
            } catch (InterruptedException e) {
                log.error(e);
            }
            
            
        } while (true);

        
    }

    public void procesarServicios(List<TablaProcesoServicio> lista, Connection con) {
        try {

            LlamadaServicio instServicio = null;
            Long tsIdAnterior = null;
            boolean debeProcesar = false;

            //MAJEAR EL DELETE

            for (TablaProcesoServicio element : lista) {
                try {
                	
                    if(!element.getCpsTransacId().equals(tsIdAnterior)){
                        if(tsIdAnterior!=null  && debeProcesar){
                            deleteExtended(con, 0L, 0L, tsIdAnterior);
                        }
                        /*
                         * Validar si debe procesar el evento Para este caso puntal debe trabajar por
                         * evento
                         */
                        
                        if (validarEvento(con, 0L, 0L, element.getCpsTransacId())) {
                            
                            debeProcesar = true;
                            log.info("Instanciando clase: " + element.getCpnClase());
//                            instServicio = ClassFactory.instanciar(element.getCpnClase());

                            instServicio = ClassFactory.instanciar(element.getCpnClase());
                                                        
                            if(validarEstadoEvento(con, element.getCpsId(), element.getCpsCpnId(), element.getCpsTransacId())) {
                            	instServicio.llamar(con, element);
                            }

                        }else {
                            debeProcesar = false;
                        }

                    }else {
                        if(debeProcesar){
                            log.info("Instanciando clase: " + element.getCpnClase());
//                            instServicio = ClassFactory.instanciar(element.getCpnClase());
                            instServicio = ClassFactory.instanciar(element.getCpnClase());
                            if(validarEstadoEvento(con, element.getCpsId(), element.getCpsCpnId(), element.getCpsTransacId())) {
                            	instServicio.llamar(con, element);
                            }
                            
                        }

                    }

                    tsIdAnterior = element.getCpsTransacId();
                } catch (Exception e) {
                    // Borra el registro bloqueado
                    deleteExtended(con, 0L, 0L, element.getCpsTransacId());
                    log.error(e);
                } catch (Throwable e) {
					// TODO Auto-generated catch block
					log.error(e);
				}
            }

            if (tsIdAnterior != null && debeProcesar) {
                deleteExtended(con, 0L, 0L, tsIdAnterior);
            }

        } catch (Exception e) {
            log.error(e);
        }
    }

    /*
     * Valida si debe procesar el evento Si la insercion falla, significa que hay
     * otro servicio procesandolo en ese momento
     */
    private Boolean validarEvento(Connection con, Long cps_id, Long cps_cpn_id, Long transac_id) {

//        log.info("Iniciando validacion `para procesar el evento...");

        Boolean result = false;

        // PreparedStatement psInsert = null;

        /*
         * Segun el servicio se debe variar la clave con el que inserta en la extendida
         * Clave: CPS_ID, CPS_CPN_ID, CPS_TRANSAC_ID
         */

        String sqlInsert = "     INSERT INTO CUP_PROCESOSERVICIOS_EXT (                            "
                + "        CPS_ID,                                                        " + // 1
                "        CPS_CPN_ID,                                                    " + // 2
                 "        CPS_TRANSAC_ID,                                                " + // 3
                "        CPS_FECHAPROCESAMIENTO                                        " + // 4
                "        ) VALUES (                                                     "
                + "        ?,                                                " + // 1
                 "        ?,                                                             " + // 2
                "        ?,                                                             " + // 3
                "        SYSTIMESTAMP)                                                  "; // 4

        try (PreparedStatement psInsert = con.prepareStatement(sqlInsert, PreparedStatement.RETURN_GENERATED_KEYS)
            ){
            psInsert.setLong(1, cps_id);
            psInsert.setLong(2, cps_cpn_id);
             psInsert.setLong(3, transac_id);
            psInsert.execute();

            result = true;
        

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            log.warn("Evento ID:" + cps_cpn_id.toString() + " esta siendo procesado por otro POD");
            log.error(e);
        }

        return result;

    }
    
    private Boolean validarEstadoEvento(Connection con, Long cps_id, Long cps_cpn_id, Long transac_id) {

      log.info("Comenzando validacion estado evento...");

      Boolean result = false;

    //   PreparedStatement psSelect = null;
      ResultSet rs = null;

      /*
       * Segun el servicio se debe variar la clave con el que inserta en la extendida
       * Clave: CPS_ID, CPS_CPN_ID, CPS_TRANSAC_ID
       */
      
      String sqlVales =
              " SELECT PROC.CPS_ESTADO                               " +
              " FROM CUP_PROCESOSERVICIOS PROC                       " +
              " WHERE CPS_ID = ?	                                 " +
              " AND CPS_CPN_ID = ?       							 " +
              " AND CPS_TRANSAC_ID = ?                               ";

          log.warn(sqlVales);


      try (
          PreparedStatement psSelect = con.prepareStatement(sqlVales, PreparedStatement.RETURN_GENERATED_KEYS);
      ){
          psSelect.setLong(1, cps_id);
          psSelect.setLong(2, cps_cpn_id);
          psSelect.setLong(3, transac_id);
          rs = psSelect.executeQuery();
          
          while (rs.next()) {
        	log.info("El estado del evento es : ' " + rs.getString(1) + " '");
        	if("OK".equals(rs.getString(1))) {
                log.warn("---------------------- ERROR ------------------------");
        		log.warn("Evento ID:" + cps_cpn_id.toString() + " ya ha sido procesado por otro POD");
                log.warn("-----------------------------------------------------");
        		result = false;
        	}else {
        		result = true;
                log.warn("-------------------- EN PROCESO ---------------------");
        		log.info("Evento ID:" + cps_cpn_id.toString() + " se encuentra listo para procesar!");
                log.warn("-----------------------------------------------------");
        	}
        	
          }

      } catch (SQLException e) {
          log.warn("Evento ID:" + cps_cpn_id.toString() + " ya ha sido procesado por otro POD");
          log.error(e);
          result = false;
      }
      
      return result;

  }


    private void deleteExtended(Connection con, Long cps_id, Long cps_cpn_id, Long transac_id) {

        // PreparedStatement psDelete = null;

        String sqlDelete = "     DELETE CUP_PROCESOSERVICIOS_EXT"
                + "     WHERE  CPS_ID = ?                                                 " + // 1
                "     AND CPS_CPN_ID = ?                            " + // 2
                 "     AND CPS_TRANSAC_ID = ?                            "; // 3

        try (PreparedStatement psDelete = con.prepareStatement(sqlDelete, PreparedStatement.RETURN_GENERATED_KEYS)){
            psDelete.setLong(1, cps_id);
            psDelete.setLong(2, cps_cpn_id);
            psDelete.setLong(3, transac_id);

            psDelete.execute();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            log.error(e);
        }

    }

}
