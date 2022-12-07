package com.arcor.cup.service;
// import cup.utils.ParametrosUtils;

// import arcor.cup.constantes.Constantes;

// import arcor.cup.constantes.ConstantesServicios;

// import com.cardinal.webservices.SendXML;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

// import cup.jde.altaimagen.TransaccionIn;
// import cup.jde.altaimagen.TransaccionOut;
// import cup.jde.altaimagen.TransaccionesInputVO;
// import cup.jde.altaimagen.TransaccionesOutput;

// import cup.jde.imgManager.CUPImagenManagerService;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.MarshalException;
import javax.xml.datatype.DatatypeConfigurationException;


import javax.xml.soap.SOAPException;

import javax.xml.ws.WebServiceRef;

// import jp595402.bssv.e1.oracle.ConfirmaAltaImagenPublish;
// import jp595402.bssv.e1.oracle.ConfirmaAltaTransaccion;
// import jp595402.bssv.e1.oracle.TransaccionInternal;
// import jp595402.bssv.e1.oracle.TransaccionPublish;

import org.apache.axis.AxisFault;
import org.apache.log4j.Logger;

import com.arcor.libs.cardinal.cup.webservices.SendXML;
import com.arcor.libs.constantesutils.cup.Constantes;
import com.arcor.libs.constantesutils.cup.ConstantesServicios;
import com.arcor.libs.dbutils.cup.DbUtilsCup;
import com.arcor.libs.parametrosutils.cup.ParametrosUtils;

// import procesoservicio.Proceso;

import com.arcor.libs.procesoservicios.cup.comunes.EstadosLlamadas;
import com.arcor.libs.procesoservicios.cup.comunes.RetornoLlamadas;

import com.arcor.libs.procesoservicios.cup.estructuras.TablaProcesoServicio;
import com.arcor.libs.procesoservicios.cup.excepciones.ServicioException;
import com.arcor.libs.procesoservicios.cup.template.LlamadaServicio;
import com.arcor.libs.procesoservicios.cup.template.RespuestaValidacionIntervalo;
import com.arcor.libs.serviciosexternos.cup.jde.altaimagen.TransaccionIn;
import com.arcor.libs.serviciosexternos.cup.jde.altaimagen.TransaccionOut;
import com.arcor.libs.serviciosexternos.cup.jde.altaimagen.TransaccionesInputVO;
import com.arcor.libs.serviciosexternos.cup.jde.altaimagen.TransaccionesOutput;

// import procesoservicio.excepciones.ServicioException;

// import procesoservicio.parserobj.altamodifmetadata.BodyAltaModif;
// import procesoservicio.parserobj.altamodifmetadata.EnvelopeAltaModif;
// import procesoservicio.parserobj.altamodifmetadata.HeaderAltaModif;

import com.arcor.libs.procesoservicios.cup.parserobj.altamodifmetadata.BodyAltaModif;
import com.arcor.libs.procesoservicios.cup.parserobj.altamodifmetadata.EnvelopeAltaModif;
import com.arcor.libs.procesoservicios.cup.parserobj.altamodifmetadata.HeaderAltaModif;

// import procesoservicio.parserobj.imagenalta.HeaderImagenAlta;

// import procesoservicio.template.LlamadaServicio;
// import procesoservicio.template.RespuestaValidacionIntervalo;

public class JdeAltaModifMetadata extends LlamadaServicio {

    private static final Logger log = Logger.getLogger(JdeAltaModifMetadata.class.getName());

    private static String url = null;
    private static final int COLA_MATCHING = 6;

    private String serviceTag = "orac:WS_AltaModif_Metadata";
    private String serviceResponseTag = "WS_AltaModif_MetadataResponse";

    private static final String PARAMETRO_BD = "ENDPOINT_ALTA_MODIF_METADATA";

    //private static String codEmpresaAnterior;

//    @WebServiceRef
//    private static CUPImagenManagerService cUPImagenManagerService;

    public void inicio() throws MalformedURLException {
        if (url == null) {
            this.url = ParametrosUtils.obtenerParametro(transaction, PARAMETRO_BD );
        }
        log.warn("JDEAltaModifMetadara URL Inicio: " + url);
    }

    public RetornoLlamadas ejecutar1(TablaProcesoServicio transac) {
        RetornoLlamadas retorno = new RetornoLlamadas();
        retorno.setEstado(EstadosLlamadas.OK);

        TransaccionesOutput response = null;
        String responseStr = "";

        try {

            response = consumirServicio(transac);

            for (TransaccionOut transaccionOut : response.getTransaccion()) {
                if (EstadosLlamadas.ERROR.toString().equals(transaccionOut.getResultado())) {
                    retorno.setEstado(EstadosLlamadas.ERROR);
                    retorno.setComentario(transaccionOut.getMensajeError());
                }
            }
        } catch (ServicioException e) {
            log.warn(e);
            retorno.setEstado(EstadosLlamadas.CAIDO);
            retorno.setComentario(e.getMessage());
        } catch (MarshalException e) {
            log.warn(e);
            retorno.setEstado(EstadosLlamadas.ERROR);
            retorno.setComentario(Constantes.ERROR_MARSHAL_EXCEPTION.getValor() +
                                  e.getMessage());
        } catch (Exception e) {
            log.warn(e);
            retorno.setEstado(EstadosLlamadas.ERROR);
            retorno.setComentario(e.getMessage());
        }

        log.warn("SALIDA " + transac.getCpsId() + " " +
                    retorno.getEstado());
        return retorno;
    }

    public TransaccionesOutput consumirServicio(TablaProcesoServicio transac) throws SQLException,
                                                                                     Exception,
                                                                                     DatatypeConfigurationException,

            AxisFault, ServicioException, SOAPException {
        TransaccionesOutput response;
        String responseStr;
        /*
         * Verifica url
         */
        if (this.url == null)
            throw new Exception(Constantes.VERIFICAR_ENDPOINT.getValor() +
                                " " + PARAMETRO_BD);

        /*
         * SETEA PARAMETROS PARA WS
         */
        TransaccionesInputVO transaccionesInput = new TransaccionesInputVO();

        log.warn("CPSTRANSACID " + transac.getCpsTransacId() == null ? "" :
                    String.valueOf(transac.getCpsTransacId()));

        TransaccionIn transaccion =
            this.getTransaccion(transac.getCpsTransacId());

        if (transaccion == null)
            throw new Exception(Constantes.ERROR_TRANSACCION_NO_ENCONTRADA.getValor() + " con transac_id " + transac.getCpsTransacId());

        transaccion.setOperacion(transac.getCpsDatos());
        transaccionesInput.getTransacciones().add(transaccion);

        /*
         * Configura llamada
         */
        log.warn("Body");
        String body = this.toXml(transaccionesInput);
        body = "<" + serviceTag + ">" + body + "</" + serviceTag + ">";
        body = HeaderAltaModif.getHeaderRequest3(super.username, super.password, body);
        log.warn(body);
        SendXML send = new SendXML();
        send.setHostAdress(url);

        Long s = System.currentTimeMillis();
        log.warn("Invocando WS JDEAltaModifMetadata");
        send.setSoapRequest(body);
        send.call();
        log.warn("Tiempo respuesta milisegundos: " +
                    (System.currentTimeMillis() - s));

        log.warn("Enviando respuesta");
        responseStr = send.getSoapResponse();

        log.warn("Respuesta");
        log.warn(responseStr);

        if (responseStr == null || "".equals(responseStr)) {
            throw new ServicioException();
        }

        response = this.parseResponse(responseStr);
        return response;
    }

    private TransaccionIn getTransaccion(Long transacID) throws SQLException,
                                                                DatatypeConfigurationException,
                                                                Exception {
        TransaccionIn ret = null;
        PreparedStatement ps2 = null;
        ResultSet trsRes = null;
        PreparedStatement ps3 = null;
        ResultSet valeRes = null;
        PreparedStatement ps = null;
        ResultSet result = null;


        /*
            * Obtengo las transacicones relacionadas
            */
        try {
            String sqlTr =
                " SELECT TRANSAC_ID    FROM TS_TRANSAC TSC                  " +
                " WHERE TSC.IS_TS_TRANSAC IN             " +
                "   (SELECT RC.ID_APLICA_OC                " +
                "   FROM CUP.TS_TRANSAC TS,                " +
                "     CUP.RECEPCION_COMPROBANTE RC         " +
                "   WHERE TS.IS_TS_TRANSAC = RC.IS_TS_TRANSAC " +
                "   AND TS.TRANSAC_ID = ?                  " +
                "   )                                      ";

            log.warn(sqlTr);
            ps2 = transaction.prepareStatement(sqlTr, PreparedStatement.RETURN_GENERATED_KEYS);
            ps2.setLong(1, transacID);
            trsRes = ps2.executeQuery();
            List<Long> trs = new ArrayList<Long>();
            while (trsRes.next()) {
                Long tr = trsRes.getLong(1);
                if (tr != null) {
                    trs.add(tr);
                }
            }

            /*
                * Obtengo vales
                */
            String sqlVales =
                " SELECT        RECEP.PRDOC,                           " +
                "               RECEP.PRDCT,                           " +
                "               RECEP.PRKCOO                           " +
                " FROM CUP.RECEPCION RECEP                             " +
                " LEFT JOIN CUP.TS_TRANSAC TS                          " +
                "       ON TS.NRO_TRANSACCION = RECEP.NRO_TRANSACCION  " +
                " WHERE TS.TRANSAC_ID   = ?                            " +
                " GROUP BY PRDOC, PRDCT, PRKCOO                        ";

            log.warn(sqlVales);
            ps3 = transaction.prepareStatement(sqlVales, PreparedStatement.RETURN_GENERATED_KEYS);
            ps3.setLong(1, transacID);
            valeRes = ps3.executeQuery();
            List<Vale> vales = new ArrayList<Vale>();
            while (valeRes.next()) {
                Vale vale = new Vale();
                vale.prdoc = valeRes.getLong(1);
                vale.prdct = valeRes.getString(2);
                vale.prkcoo = valeRes.getString(3);
                vales.add(vale);
            }

            /*
                * Obtengo transaccion y sus datos
                */
            String sql =
                " SELECT TS.TRANSAC_ID,                              " +
                "   TS.NRO_TRANSACCION,                              " +
                "   TS.COD_EMPRESA,                                  " +
                "   TS.TAX_ID,                                       " +
                "   TS.COD_COMPROBANTE,                              " +
                "   TS.CUIT,                                         " +
                "   COMP.COM_DESCRIPCION,                            " +
                "   TS.FEC_COMP,                                     " +
                "   TS.NRO_COMP,                                     " +
                "   TS.IMP_TOTAL,                                    " +
                "   PAI.PAI_CODIGO,                                  " +
                "   TS.TIPO_OC,                                      " +
                "   TS.NRO_OC,                                       " +
                "   TS.ESTADO,                                       " +
                "   INFO.CTI_FECHAVENCIMIENTO,                       " +
                "   TS.CARPETA,                                      " +
                "   TS.FEC_HORA_TRANSAC,                             " +
                "   CTI_CODIGOFISCAL," +
                "   regexp_substr(INFO.CTI_CODIGOFISCAL,'.{1,14}$' ) AS CTI_CODIGOFISCAL14," +
                "   INFO.CTI_FEC_VENC_CODFISCAL,                     " +
                "   INFO.CTI_VALIDOFISCAL,                           " +
                "   INFO.CTI_FECHAINGRESO,                           " +
                "   TS.COD_PROVEEDOR ,                               " +
                "   F.ABALPH AS PROVEEDOR,                           " +
                "   TIPO.LETRA,                                      " +
                "   TS.ID_COMP,                                      " +
                "   COLA.CCT_CODIGO,                                 " +
                "   INFO.CTI_WEBCENTER_ID,                           " +
                "   TS.TIPO_REFERENCIA,                              " +
                "   TS.REFERENCIA_DESC                               " +
                " FROM CUP.TS_TRANSAC TS,                            " +
                "   CUP.CUP_TRANSAC_INFO INFO,                       " +
                "   CUP.CUP_COMPANIA COMP,                           " +
                "   CUP.CUP_PAIS PAI,                                " +
                "   CUP.TV_TIPO_COMP TIPO,                           " +
                "   CUP.F0101 F,                                     " +
                "   CUP.CUP_COLA_TRANSAC COLA                        " +
                " WHERE TS.TRANSAC_ID      = INFO.CTI_TRANSAC_ID (+) " +
                " AND trim(TS.COD_EMPRESA) = trim(COMP.COM_CODIGO)   " +
                " AND TS.COD_PAIS          = PAI.PAI_CODIGO          " +
                " AND TS.COD_COMPROBANTE   = TIPO.COD_COMPROBANTE    " +
                " AND TS.COD_PAIS          = TIPO.COD_PAIS           " +
                " AND TS.COD_PROVEEDOR     = F.ABAN8                 " +
                " AND INFO.CTI_CCT_ID      = COLA.CCT_ID             " +
                " AND TS.TRANSAC_ID        = ?                       ";


            log.warn(sql);


            ps = transaction.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
            ps.setLong(1, transacID);

            result = ps.executeQuery();
            if (result.next()) {
                ret = new TransaccionIn();

                ret.setId(result.getLong("TRANSAC_ID"));
                String paisCodigo = result.getString("PAI_CODIGO");
                ret.setPais(paisCodigo);
                if ( "AR".equals(paisCodigo)){
                    log.warn("PAIS ARGENTINA ENVIO CUIT");
                Long cuit = result.getLong("CUIT");
                ret.setCodigoFiscal(cuit == null ? null :
                                    String.valueOf(cuit));
                }
                else{
                    log.warn("PAIS NO ARGENTINA ENVIO TAX_ID");
                    ret.setCodigoFiscal(result.getString("TAX_ID") == null ? "" :
                                   result.getString("TAX_ID").trim());
                }
                ret.setRazonSocial(result.getString("PROVEEDOR") == null ? "" :
                                   result.getString("PROVEEDOR").trim());

                Date fechaFactura = result.getDate("FEC_COMP");
                ret.setFechaFactura(fechaFactura);
                ret.setNroComprobante(result.getString("ID_COMP"));
                ret.setImporte(result.getBigDecimal("IMP_TOTAL"));
                ret.setTipoOC(result.getString("TIPO_OC"));
                ret.setNumeroOC(result.getInt("NRO_OC"));
                ret.setEstadoTransaccion(result.getString("CCT_CODIGO"));
                ret.setFechaVencimiento(result.getDate("CTI_FECHAVENCIMIENTO"));
                ret.setCarpeta(result.getString("CARPETA"));

                /*Date fechaHoraTransac = result.getDate("FEC_HORA_TRANSAC");
                   ret.setFechaTransaccion(fechaHoraTransac);*/
                Date fechaTransaccion = result.getDate("FEC_HORA_TRANSAC");
                ret.setFechaTransaccion(fechaTransaccion);

                ret.setCodigoFiscal(result.getString("CTI_CODIGOFISCAL"));

                Date fechaVenicimientoCodigoFiscal =
                    result.getDate("CTI_FEC_VENC_CODFISCAL");
                ret.setFechaVencimientoCodigoFiscal(fechaVenicimientoCodigoFiscal);

                //Date fechaIngreso = result.getDate("CTI_FECHAINGRESO");

                //
                Long idWebcenter = result.getLong("CTI_WEBCENTER_ID");
                Date d = null;
                if (idWebcenter != null && idWebcenter > 0) {
                    d = new Date();
                }

                ret.setFechaEscaneo(d);

                log.warn("Transaccion " + transacID + " fecha escaneo " +
                            ret.getFechaEscaneo());

                ret.setNroProveedor(result.getInt("COD_PROVEEDOR"));

                Map valoresValidoFiscal = new HashMap<String, Integer>();
                valoresValidoFiscal.put("SVAL", 2);
                valoresValidoFiscal.put("NA", 3);
                valoresValidoFiscal.put("OK", 0);
                valoresValidoFiscal.put("NOOK", 1);
                valoresValidoFiscal.put("ERROR", 4);
                valoresValidoFiscal.put("SDAT", 2);

                String validoFiscalStr = result.getString("CTI_VALIDOFISCAL");
                validoFiscalStr =
                        validoFiscalStr == null ? "SVAL" : validoFiscalStr.trim();
                log.warn("JDEALTAMODIFMETADATA validoFiscalStr: " +
                            validoFiscalStr);
                if (validoFiscalStr != null) {
                    Integer validoFiscal =
                        (Integer)valoresValidoFiscal.get(validoFiscalStr);
                    log.warn("JDEALTAMODIFMETADATA validoFiscal: " +
                                validoFiscal);
                    if (validoFiscal == null) {
                        validoFiscal = new Integer(2);
                    }
                    log.warn("JDEALTAMODIFMETADATA validoFiscal: " +
                                validoFiscal);
                    ret.setValidoFiscal(String.valueOf(validoFiscal));
                }

                ret.setIdImpositivo(result.getString("CUIT"));
                ret.setTipoDeComprobante(result.getString("COD_COMPROBANTE"));

                Long nroTransaccion = result.getLong("NRO_TRANSACCION");
                //ret.setDatosAdicionales(this.inflateDatosAdicionales(trs,vales,nroTransaccion));

                ret.setReferenciaDescr(result.getString("REFERENCIA_DESC"));
                ret.setReferenciaTipos(result.getString("TIPO_REFERENCIA"));
                Referencias rr =
                    getReferencias(ret.getReferenciaTipos(), ret.getReferenciaDescr());
                
                
                //Obtengo XML original
                String xmlOriginal = DbUtilsCup.getXMlOriginalTransaccionTemporal(transaction,transacID);
                
                ret.setDatosAdicionales(this.inflateDatosAdicionales(trs,
                                                                     vales, rr,
                                                                     nroTransaccion,xmlOriginal));


                ret.setLetra(result.getString("LETRA"));
                ret.setCompania(result.getString("COD_EMPRESA"));
            } else {
                throw new SQLException(Constantes.ERROR_TRANSACCION_NO_ENCONTRADA.getValor() +
                                       " con transac_id " + transacID +
                                       ", para el criterio de busqueda de transacciones para Alta Modificacion Metadata");
            }
        } catch (Exception e) {
            log.warn(e);
            throw e;
        } finally {
            try {
                ps.close();
            } catch (SQLException e) {
            }
            try {
                result.close();
            } catch (SQLException e) {
            }
            try {
                ps2.close();
            } catch (SQLException e) {
            }
            try {
                trsRes.close();
            } catch (SQLException e) {
            }
            try {
                ps3.close();
            } catch (SQLException e) {
            }
            try {
                valeRes.close();
            } catch (SQLException e) {
            }
        }

        log.warn("Fin getTransaction");
        return ret;
    }


    public void ejecutar(TablaProcesoServicio transac) {
        RetornoLlamadas retorno = new RetornoLlamadas();
        retorno.setEstado(EstadosLlamadas.OK);
        TransaccionesOutput response;

//        if (Proceso.serviciosJdeAltaModif.size() == 0) {
//            return;
//        }

        /*
         * Obtiene el lote a procesar
         */
//        int desde = 0;
        String codEmpresaAnterior = null;
//        List<TablaProcesoServicio> tmpList =
//            new ArrayList<TablaProcesoServicio>();
//        List<TablaProcesoServicio> copia =
//            new ArrayList<TablaProcesoServicio>(Proceso.serviciosJdeAltaModif);

//        for (TablaProcesoServicio servicio : copia) {
            try {
                log.warn("Verificando si hay que procesar transaccion " +
                		transac.getCpsTransacId());

                //Verifica la Cola de la Transaccion
                if (transac.getCola() >= COLA_MATCHING) {
                    //Verifica si hay que procesarlo
                    RespuestaValidacionIntervalo res = hayQueProcesar(transac);
                    if (res.isHayQueProcesar()) {
                        //Verifica que no sea el primero o que sea el mismo codigo de empresa anterior
                        log.warn("Hay que procesar transaccion " +
                        		transac.getCpsTransacId());
                        if (codEmpresaAnterior != null &&
                        		transac.getTsCodEmpresa().equals(codEmpresaAnterior)) {
//                            tmpList.add(transac);
                        } else {
//                            if (codEmpresaAnterior == null) {
//                                tmpList.add(servicio);
//                            }
                            codEmpresaAnterior = transac.getTsCodEmpresa();
                        }
                    } else {
                        log.warn("Salteando por intervalo transaccion " +
                        		transac.getCpsTransacId());
                        registrarValidacion(transaction,
                        					transac.getCpsId(),
                                            res.getFechaHayQueProcesar()
                                            );
                        log.warn("commit por intervalo START");
                        // transaction.commit();
                        log.warn("commit por intervalo END");
//                        Proceso.serviciosJdeAltaModif.remove(servicio);
                    }
                } else {
                    log.warn("Salteando por Cola Anterior a MATCHING: " +
                    			transac.getCpsTransacId());

                }


//                if (tmpList.size() >= Proceso.MAX_POR_LLAMADA) {
//                    break;
//                }

            } catch (Exception e) {
                log.warn("Error al procesar en JdeAltaMOdifMetadata.: " +
                			transac.getCpsTransacId(), e);
            }
//        }

//        if (tmpList.size() > 0) {
            String username = transac.getComEntorno();
            String password = transac.getComPassword();

            try {
                /*
                 * Verifica url
                 */
                if (this.url == null)
                    throw new Exception(Constantes.VERIFICAR_ENDPOINT.getValor() +
                                        " " + PARAMETRO_BD);

                //Arma input de servicio
                TransaccionesInputVO in = new TransaccionesInputVO();
//                for (TablaProcesoServicio servicio : tmpList) {
                    try {
                        TransaccionIn servicioIn =
                            getData(transac.getCpsTransacId());
                        log.warn("Enviando transaccion " +
                        			transac.getCpsTransacId());
                        in.getTransacciones().add(servicioIn);
                    } catch (Exception e) {
                        log.warn(e);
                        setException(transac, EstadosLlamadas.ERROR, e);
                        throw e;
                    }
//                }

                /*
                 * Parsea request a xml, agrega tag, user y contrase�a
                 */
                String body = this.toXml(in);
                body = "<" + serviceTag + ">" + body + "</" + serviceTag + ">";

                body = HeaderAltaModif.getHeaderRequest3(username, password, body);

                log.warn(body);

                /*
                 * Llamada a WS
                 */
                SendXML send = new SendXML();
                send.setHostAdress(this.url);
                log.warn("JDEAltaModifMetada URL: " + this.url);
                send.setSoapRequest(body);
                send.call();

                /*
                 * Obtiene y parsea respuesta
                 */
                String responseStr = send.getSoapResponse();
                if (responseStr == null) {
                    throw new ServicioException();
                }

                log.warn(responseStr);
                response = parseResponse(responseStr);
                setResultado(transac, response);
            } catch (ServicioException e) {
                log.warn(e);
                try {
					setException(transac, EstadosLlamadas.CAIDO, e);
				} catch (SQLException e1) {
					log.warn(e1);
				}
            } catch (Exception e) {
                log.warn(e);
                try {
					setException(transac, EstadosLlamadas.ERROR, e);
				} catch (SQLException e1) {
					log.warn(e1);
				}
            }
//        } else {
//            return;
//        }

        //Elimina elementos procesados
//        Proceso.serviciosJdeAltaModif.removeAll(tmpList);
    }

    public void fin() throws Throwable {
        //
    }


    private TransaccionIn getData(Long transacID) throws SQLException,
                                                         DatatypeConfigurationException {
        TransaccionIn ret = null;
        PreparedStatement ps2 = null;
        ResultSet trsRes = null;
        PreparedStatement ps3 = null;
        ResultSet valeRes = null;
        PreparedStatement ps = null;
        ResultSet result = null;


        /*
         * Obtengo las transacicones relacionadas
         */
        try {
            String sqlTr =
                " SELECT TRANSAC_ID                        " + " FROM CUP.TS_TRANSAC TSC                  " +
                " WHERE TSC.IS_TS_TRANSAC IN             " +
                "   (SELECT RC.ID_APLICA_OC                " +
                "   FROM CUP.TS_TRANSAC TS,                " +
                "     CUP.RECEPCION_COMPROBANTE RC         " +
                "   WHERE TS.IS_TS_TRANSAC = RC.IS_TS_TRANSAC " +
                "   AND TS.TRANSAC_ID = ?                  " +
                "   )                                      ";

            ps2 = transaction.prepareStatement(sqlTr, PreparedStatement.RETURN_GENERATED_KEYS);
            ps2.setLong(1, transacID);
            trsRes = ps2.executeQuery();
            List<Long> trs = new ArrayList<Long>();
            while (trsRes.next()) {
                Long tr = trsRes.getLong(1);
                if (tr != null) {
                    trs.add(tr);
                }
            }

            /*
             * Obtengo vales
             */
            String sqlVales =
                " SELECT RECEP.PRDOC,                           " +
                " RECEP.PRDCT,                           " +
                " RECEP.PRKCOO                           " +
                " FROM CUP.RECEPCION RECEP                             " +
                " LEFT JOIN CUP.TS_TRANSAC TS                          " +
                " ON TS.NRO_TRANSACCION = RECEP.NRO_TRANSACCION  " +
                " WHERE TS.TRANSAC_ID   = ?                            " +
                " GROUP BY PRDOC, PRDCT, PRKCOO                        ";

            ps3 = transaction.prepareStatement(sqlVales, PreparedStatement.RETURN_GENERATED_KEYS);
            ps3.setLong(1, transacID);
            valeRes = ps3.executeQuery();
            List<Vale> vales = new ArrayList<Vale>();
            while (valeRes.next()) {
                Vale vale = new Vale();
                vale.prdoc = valeRes.getLong(1);
                vale.prdct = valeRes.getString(2);
                vale.prkcoo = valeRes.getString(3);
                vales.add(vale);
            }

            /*
             * Obtengo transaccion y sus datos
             */
            String sql =
                " SELECT TS.TRANSAC_ID,                              " +
                "   TS.NRO_TRANSACCION,                              " +
                "   TS.COD_EMPRESA,                                  " +
                "   TS.TAX_ID,                                       " +
                "   TS.COD_COMPROBANTE,                              " +
                "   TS.CUIT,                                         " +
                "   COMP.COM_DESCRIPCION,                            " +
                "   TS.FEC_COMP,                                     " +
                "   TS.NRO_COMP,                                     " +
                "   TS.IMP_TOTAL,                                    " +
                "   PAI.PAI_CODIGO,                                  " +
                "   TS.TIPO_OC,                                      " +
                "   TS.NRO_OC,                                       " +
                "   TS.ESTADO,                                       " +
                "   INFO.CTI_FECHAVENCIMIENTO,                       " +
                "   TS.CARPETA,                                      " +
                "   TS.FEC_HORA_TRANSAC,                             " +
                "   CTI_CODIGOFISCAL," +
                "   regexp_substr(INFO.CTI_CODIGOFISCAL,'.{1,14}$' ) AS CTI_CODIGOFISCAL14," +
                "   INFO.CTI_FEC_VENC_CODFISCAL,                     " +
                "   INFO.CTI_VALIDOFISCAL,                           " +
                "   INFO.CTI_FECHAINGRESO,                           " +
                "   TS.COD_PROVEEDOR ,                               " +
                "   F.ABALPH AS PROVEEDOR,                           " +
                "   TIPO.LETRA,                                      " +
                "   TS.ID_COMP,                                      " +
                "   COLA.CCT_CODIGO,                                 " +
                "   TS.TIPO_REFERENCIA,                              " +
                "   TS.REFERENCIA_DESC                               " +
                " FROM CUP.TS_TRANSAC TS,                            " +
                "   CUP.CUP_TRANSAC_INFO INFO,                       " +
                "   CUP.CUP_COMPANIA COMP,                           " +
                "   CUP.CUP_PAIS PAI,                                " +
                "   CUP.TV_TIPO_COMP TIPO,                           " +
                "   CUP.F0101 F,                                     " +
                "   CUP.CUP_COLA_TRANSAC COLA                        " +
                " WHERE TS.TRANSAC_ID      = INFO.CTI_TRANSAC_ID (+) " +
                " AND trim(TS.COD_EMPRESA) = trim(COMP.COM_CODIGO)   " +
                " AND TS.COD_PAIS          = PAI.PAI_CODIGO          " +
                " AND TS.COD_COMPROBANTE   = TIPO.COD_COMPROBANTE    " +
                " AND TS.COD_PAIS          = TIPO.COD_PAIS           " +
                " AND TS.COD_PROVEEDOR     = F.ABAN8                 " +
                " AND INFO.CTI_CCT_ID      = COLA.CCT_ID             " +
                " AND TS.TRANSAC_ID        = ?                       ";


            ps = transaction.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
            ps.setLong(1, transacID);

            result = ps.executeQuery();
            if (result.next()) {
                ret = new TransaccionIn();

                //TODO VERIFICAR TODOS LOS CAMPOS
                ret.setId(result.getLong("TRANSAC_ID"));

                String paisCodigo = result.getString("PAI_CODIGO");
                ret.setPais(paisCodigo);
                if ( "AR".equals(paisCodigo)){
                    log.warn("PAIS ARGENTINA ENVIO CUIT");
                Long cuit = result.getLong("CUIT");
                    ret.setIdImpositivo(cuit == null ? null :
                                    String.valueOf(cuit));
                ret.setCodigoFiscal(cuit == null ? null :
                                    String.valueOf(cuit));
                }
                else{
                    log.warn("PAIS NO ARGENTINA ENVIO TAX_ID");
                    ret.setCodigoFiscal(result.getString("TAX_ID") == null ? "" :
                                   result.getString("TAX_ID").trim());
                    ret.setIdImpositivo(result.getString("TAX_ID") == null ? "" :
                                   result.getString("TAX_ID").trim());
                }
                ret.setRazonSocial(result.getString("PROVEEDOR") == null ? "" :
                                   result.getString("PROVEEDOR").trim());
                Date fechaFactura = result.getDate("FEC_COMP");
                ret.setFechaFactura(fechaFactura);
                ret.setNroComprobante(result.getString("ID_COMP"));
                ret.setImporte(result.getBigDecimal("IMP_TOTAL"));
                ret.setTipoOC(result.getString("TIPO_OC"));
                ret.setNumeroOC(result.getInt("NRO_OC"));
                ret.setEstadoTransaccion(result.getString("CCT_CODIGO"));
                ret.setFechaVencimiento(result.getDate("CTI_FECHAVENCIMIENTO"));
                ret.setCarpeta(result.getString("CARPETA"));

                /*Date fechaHoraTransac = result.getDate("FEC_HORA_TRANSAC");
                ret.setFechaTransaccion(fechaHoraTransac);*/
                Date fechaTransaccion =
                    result.getTimestamp("FEC_HORA_TRANSAC");
                ret.setFechaTransaccion(fechaTransaccion);

                ret.setCodigoFiscal(result.getString("CTI_CODIGOFISCAL"));

                Date fechaVenicimientoCodigoFiscal =
                    result.getDate("CTI_FEC_VENC_CODFISCAL");
                ret.setFechaVencimientoCodigoFiscal(fechaVenicimientoCodigoFiscal);

                Date fechaIngreso = result.getTimestamp("CTI_FECHAINGRESO");
                ret.setFechaEscaneo(fechaIngreso);

                ret.setNroProveedor(result.getInt("COD_PROVEEDOR"));

                Map valoresValidoFiscal = new HashMap<String, Integer>();
                valoresValidoFiscal.put("SVAL", 2);
                valoresValidoFiscal.put("NA", 3);
                valoresValidoFiscal.put("OK", 0);
                valoresValidoFiscal.put("NOOK", 1);
                valoresValidoFiscal.put("ERROR", 4);
                valoresValidoFiscal.put("SDAT", 2);

                String validoFiscalStr = result.getString("CTI_VALIDOFISCAL");
                validoFiscalStr =
                        validoFiscalStr == null ? "SVAL" : validoFiscalStr.trim();
                if (validoFiscalStr != null) {
                    Integer validoFiscal =
                        (Integer)valoresValidoFiscal.get(validoFiscalStr);
                    log.warn("JDEALTAMODIFMETADATA validoFiscal: " +
                                validoFiscal);
                    if (validoFiscal == null) {
                        validoFiscal = new Integer(2);
                    }
                    log.warn("JDEALTAMODIFMETADATA validoFiscal: " +
                                validoFiscal);
                    ret.setValidoFiscal(String.valueOf(validoFiscal));
                }

             
                ret.setTipoDeComprobante(result.getString("COD_COMPROBANTE"));

                ret.setReferenciaDescr(result.getString("REFERENCIA_DESC"));
                ret.setReferenciaTipos(result.getString("TIPO_REFERENCIA"));
                Referencias rr =
                    getReferencias(ret.getReferenciaTipos(), ret.getReferenciaDescr());

                Long nroTransaccion = result.getLong("NRO_TRANSACCION");

                //Obtengo XML original
                String xmlOriginal = DbUtilsCup.getXMlOriginalTransaccionTemporal(transaction,transacID);
                ret.setDatosAdicionales(this.inflateDatosAdicionales(trs,
                                                                     vales, rr,
                                                                     nroTransaccion,xmlOriginal));

                ret.setLetra(result.getString("LETRA"));
                ret.setCompania(result.getString("COD_EMPRESA"));
            } else {
                throw new SQLException(Constantes.ERROR_TRANSACCION_NO_ENCONTRADA.getValor() +
                                       " con transac_id " + transacID +
                                       ", para el criterio de busqueda de transacciones para Alta Modificacion Metadata");
            }
        } finally {
            try {
                if (ps != null)
                    ps.close();
            } catch (SQLException e) {
            }
            try {
                if (result != null)
                    result.close();

            } catch (SQLException e) {
            }
            try {
                if (ps2 != null)
                    ps2.close();

            } catch (SQLException e) {
            }
            try {
                if (trsRes != null)
                    trsRes.close();


            } catch (SQLException e) {
            }
            try {
                if (ps3 != null)
                    ps3.close();

            } catch (SQLException e) {
            }
            try {
                if (valeRes != null)
                    valeRes.close();

            } catch (SQLException e) {
            }
        }

        log.warn("Fin getData.");
        return ret;
    }

    public String toXml(TransaccionesInputVO transacciones) {
        XStream xstream = new XStream(new DomDriver());

        xstream.alias("", TransaccionesInputVO.class);
        xstream.alias("transacciones", TransaccionIn.class);

        String xml = xstream.toXML(transacciones);

        xml = xml.replace("<listaDeTransaccionesParaJDE99592>", "");
        xml = xml.replace("</listaDeTransaccionesParaJDE99592>", "");
        xml = xml.replace("<>", "");
        xml = xml.replace("</>", "");

        return xml;
    }

    public TransaccionesOutput parseResponse(String response) throws Exception {
        String nameSpace = EnvelopeAltaModif.getNameSpace(response);
        String serviceTagNameSpace = EnvelopeAltaModif.getServiceTagNameSpace(response, serviceResponseTag,"transaccion");
        serviceTagNameSpace = "ns2";

        XStream xstream = new XStream(new DomDriver());
        xstream.autodetectAnnotations(true);

        xstream.alias(nameSpace + ":Envelope", EnvelopeAltaModif.class);

        xstream.alias(nameSpace + ":Body", BodyAltaModif.class);
        xstream.alias(nameSpace + ":Header", HeaderAltaModif.class);

        xstream.aliasField(nameSpace + ":Body", EnvelopeAltaModif.class,
                           "body");
        xstream.aliasField(nameSpace + ":Header", EnvelopeAltaModif.class,
                           "header");
        xstream.aliasField(serviceTagNameSpace + ":" + serviceResponseTag,
                           BodyAltaModif.class, "estructura");

        xstream.addImplicitCollection(TransaccionesOutput.class,
                                      "transaccion");
        xstream.alias(serviceTagNameSpace + ":" + serviceResponseTag,
                      TransaccionesOutput.class);
        xstream.alias("transaccion", TransaccionOut.class);
        xstream.aliasField("ID", TransaccionOut.class, "id");

        EnvelopeAltaModif envelope =
            (EnvelopeAltaModif)xstream.fromXML(response);

        return envelope.getBody().getEstructura();
    }


    public void setResultado(TablaProcesoServicio element,
                             TransaccionesOutput response) throws SQLException {
//        for (int i = 0; i < response.getTransaccion().size(); i++) {
            TransaccionOut retServicio = response.getTransaccion().get(0);
            TablaProcesoServicio servicioOriginal = element;

            RetornoLlamadas r = new RetornoLlamadas();
            r.setEstado(retServicio.getResultado());
            r.setComentario(retServicio.getMensajeError() == null ? null :
                            retServicio.getMensajeError().trim());

            try {
                setEstado(transaction,servicioOriginal.getCpsId(), r);
            } catch (SQLException e) {
                log.warn(e);
//                transaction.rollback();
            } finally {
                // transaction.commit();
            }
//        }
    }


    /**
     *  Genera XML para datos adicionales
     */
    public static String inflateDatosAdicionales(List<Long> trs,
                                                 List<Vale> vales,
                                                 Referencias referencias,
                                                 Long nroTransaccion, String xmlOriginalTemporal) {
        String xml = "";

        xml +=
"       <![CDATA[" + "       <infoComplementaria>" + "               <version>1.0</version>";
        for (Long tr : trs) {
            if (tr != null) {
                xml += "<tr>";
                xml += tr + "</tr>";
            }
        }

        for (Vale vale : vales) {
            xml += "<vale>";
            if (vale.prdoc != null) {
                xml += "<nro>";
                xml += String.valueOf(vale.prdoc) + "</nro>";
            } else {
                xml += "<nro></nro>";
            }

            if (vale.prdct != null) {
                xml += "<tipo>";
                xml += vale.prdct == null ? "" : vale.prdct + "</tipo>";
            } else {
                xml += "<tipo></tipo>";
            }

            if (vale.prkcoo != null) {
                xml += "<comp>";
                xml += vale.prkcoo == null ? "" : vale.prkcoo + "</comp>";
            } else {
                xml += "<comp></comp>";
            }
            xml += "</vale>";
        }

        if (nroTransaccion != null) {
            xml += "<nro_transaccion>";
            xml += nroTransaccion + "</nro_transaccion>";
        } else {
            xml += "<nro_transaccion></nro_transaccion>";
        }

        if (referencias != null) {
            xml += generaXMLReferencias(referencias);
        }

        // 19/07/2018 SCD - ARCOR 512 - No se envia el XML Original
        //  if (xmlOriginalTemporal != null) {
        //    xml += "<transacTemporalXmlOriginal>";
        //    xml += xmlOriginalTemporal.replaceAll("(\r\n|\n\r|\r|\n)", "") + "</transacTemporalXmlOriginal>";
        //}  
        
        xml += "</infoComplementaria>";
        xml += "]]>";

        //        StringEscapeUtils.escapeXml11()

        return xml;
    }

    private static String generaXMLReferencias(Referencias referencias) {
        String xml = ""; // TODO type initialisation here
        if (referencias == null)
            return null;
        if (referencias.desc != null)
            xml += "<Ref>" + referencias.desc + "</Ref>";
        if (referencias.detalle != null && referencias.detalle.size() > 0) {
            xml += "<ListaTipoReferencias>";
            for (String s : referencias.detalle) {
                xml += "<tipoRef>" + s + "</tipoRef>";
            }
            xml += "</ListaTipoReferencias>";
        }
        return xml;
    }


    public void activateTransaccion(Connection transaccion) {
        if (this.transaction == null) {
            this.transaction = transaccion;
        }
    }

    public static RetornoLlamadas ejecutarRelanzarOnline(Long transacId,
                                                         Connection transaccion) {
        RetornoLlamadas retorno = new RetornoLlamadas();
        try {
            JdeAltaModifMetadata serv = new JdeAltaModifMetadata();
            TablaProcesoServicio datos = new TablaProcesoServicio();
            datos.setCpsTransacId(transacId);
            datos.setCpsDatos(ConstantesServicios.ServicioJdeAltaModifMetadata_DataRelanzar);
            serv.activateTransaccion(transaccion);
            serv.inicio();
            serv.setUserPassword(transaccion,transacId);
            retorno = serv.ejecutar1(datos);
        } catch (Exception e) {
            retorno.setEstado(EstadosLlamadas.ERROR);
            retorno.setComentario(e.getMessage());
            log.warn(e);
        }
        return retorno;
    }


    private static Referencias getReferencias(String tipos,
                                              String descripcion) {
        log.info("Get Referencias");
        Referencias rr = new Referencias();

        if (descripcion != null) {
            log.info("Get Referencias : Descripcion not null");
            rr.desc = descripcion;
        } else {
            rr.desc = "0";
        }

        if (tipos != null) {
            log.info("Get Referencias : Tipos not null");
            String t[] = tipos.split(";");
            rr.detalle = Arrays.asList(t);

        }


        /*     if (descripcion != null && tipos != null){
                   rr = new Referencias();
                   rr.desc = descripcion;
                   String t[] = tipos.split(";");
                   rr.detalle = Arrays.asList(t);
               }
                */


        return rr;
    }

    public static void main(String[] s) {
        Referencias r2 = getReferencias("P", "5555555");
        System.out.println(generaXMLReferencias(r2));
        r2 = getReferencias("P;I", "5555555");
        System.out.println(generaXMLReferencias(r2));
        r2 = getReferencias("P;", "5555555");
        System.out.println(generaXMLReferencias(r2));
        r2 = getReferencias(null, "5555555");
        System.out.println(generaXMLReferencias(r2));
        r2 = getReferencias("A", null);
        System.out.println(generaXMLReferencias(r2));
    }
}

class Vale {
    public Long prdoc;
    public String prdct;
    public String prkcoo;
}

class Referencias {
    List<String> detalle;
    String desc;
}
