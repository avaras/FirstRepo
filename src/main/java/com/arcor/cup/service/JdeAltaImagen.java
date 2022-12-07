package com.arcor.cup.service;

import com.arcor.libs.cardinal.cup.ucm.wcc12.response.FileInfo;
import com.arcor.libs.cardinal.cup.webservices.SendXML;
import com.arcor.libs.constantesutils.cup.Constantes;
import com.arcor.libs.dbutils.cup.DbUtilsCup;
import com.arcor.libs.parametrosutils.cup.ParametrosUtils;
import com.arcor.libs.procesoservicios.cup.comunes.EstadosLlamadas;
import com.arcor.libs.procesoservicios.cup.comunes.RetornoLlamadas;
import com.arcor.libs.procesoservicios.cup.estructuras.TablaProcesoServicio;
import com.arcor.libs.procesoservicios.cup.excepciones.ServicioException;
import com.arcor.libs.procesoservicios.cup.parserobj.imagenalta.BodyImagenAlta;
import com.arcor.libs.procesoservicios.cup.parserobj.imagenalta.EnvelopeImagenAlta;
import com.arcor.libs.procesoservicios.cup.parserobj.imagenalta.HeaderImagenAlta;
import com.arcor.libs.procesoservicios.cup.template.LlamadaServicio;
import com.arcor.libs.procesoservicios.cup.template.RespuestaValidacionIntervalo;
import com.arcor.libs.serviciosexternos.cup.jp595402.bssv.e1.oracle.ConfirmaAltaImagenPublish;
import com.arcor.libs.serviciosexternos.cup.jp595402.bssv.e1.oracle.ConfirmaAltaTransaccion;
import com.arcor.libs.serviciosexternos.cup.jp595402.bssv.e1.oracle.TransaccionInternal;
import com.arcor.libs.serviciosexternos.cup.jp595402.bssv.e1.oracle.TransaccionPublish;
import com.arcor.libs.utils.cup.Utils;

// No utilizado ***
//  import com.arcor.libs.procesoservicios.cup.parserobj.*;
// import com.arcor.libs.serviciosexternos.cup.jde.imgManager.CUPImagenManagerService;

// import cup.utils.DbUtils;
// import cup.utils.ParametrosUtils;
// import cup.utils.Utils;

// import arcor.cup.constantes.Constantes;
 
// import com.cardinal.ucm.wcc12.response.FileInfo;
// import com.cardinal.webservices.SendXML; 
// TEST

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

// import cup.jde.imgManager.CUPImagenManagerService;

import java.net.MalformedURLException;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;

import javax.xml.ws.WebServiceRef;

// import jp595402.bssv.e1.oracle.ConfirmaAltaImagenPublish;
// import jp595402.bssv.e1.oracle.ConfirmaAltaTransaccion;
// import jp595402.bssv.e1.oracle.TransaccionInternal;
// import jp595402.bssv.e1.oracle.TransaccionPublish;

// import oracle.jbo.server.DBTransaction;

import org.apache.commons.lang3.ArrayUtils;

// import procesoservicio.Proceso;

// import procesoservicio.comunes.EstadosLlamadas;
// import procesoservicio.comunes.RetornoLlamadas;

// import procesoservicio.estructuras.TablaProcesoServicio;

// import procesoservicio.excepciones.ServicioException;

// import procesoservicio.parserobj.imagenalta.BodyImagenAlta;
// import procesoservicio.parserobj.imagenalta.EnvelopeImagenAlta;
// import procesoservicio.parserobj.imagenalta.HeaderImagenAlta;

// import procesoservicio.template.LlamadaServicio;
// import procesoservicio.template.RespuestaValidacionIntervalo;

import org.apache.log4j.Logger;


public class JdeAltaImagen extends LlamadaServicio {

    private static final Logger log = Logger.getLogger(JdeAltaImagen.class.getName());

    log.info("commit-2");
    log.info("commit-3");

    private String serv2 = null;

<<<<<<< HEAD
    log.info("commit-6");


=======
>>>>>>> parent of 5c18ba9 (commit-6)
    private static String url = null;

    System.out.println ("commit-8 -epic1");
    
    private String serviceTag = "orac:wsAltaImagen";
    private String serviceResponseTag = "ns2:wsAltaImagenResponse";
    
    private static final String PARAMETRO_BD = "ENDPOINT_IMG_MANAGER";
    
    private String codEmpresaAnterior;
    
    // @WebServiceRef
    // private static CUPImagenManagerService cUPImagenManagerService;
    
    public void inicio() throws MalformedURLException {
        if (url == null) {
                this.url = ParametrosUtils.obtenerParametro(transaction, PARAMETRO_BD);
        }
        codEmpresaAnterior = null;
        log.warn("JdeAltaImagen URL Inicio: " + url);
    }
    
    public void ejecutar(TablaProcesoServicio transac) {
        RetornoLlamadas retorno = new RetornoLlamadas();
        retorno.setEstado(EstadosLlamadas.OK);
        ConfirmaAltaImagenPublish response;
        
        // if(Proceso.serviciosJdeAltaImagen.size() == 0) {
        //     return;
        // }
        
        /*
         * Obtiene el lote a procesar
         */
        codEmpresaAnterior = null;
        // List<TablaProcesoServicio> tmpList = new ArrayList<TablaProcesoServicio>();
        // List<TablaProcesoServicio> copia = new ArrayList<TablaProcesoServicio>(Proceso.serviciosJdeAltaImagen);
        
        // for(TablaProcesoServicio servicio: copia) {
            
            try {
                log.warn("Verificando si hay que procesar transaccion " + transac.getCpsTransacId());
                
                //Verifica si hay que procesarlo
                RespuestaValidacionIntervalo res = hayQueProcesar(transac);
                if(res.isHayQueProcesar()) {

                    //Verifico si hay que pasar Img sino pongo mensaje de que no
                    if ("S".equals(transac.getCtiPasarImagenJDE())) {

                        //Verifica que no sea el primero o que sea el mismo codigo de empresa anterior
                        log.warn("Hay que procesar transaccion " +
                                    transac.getCpsTransacId());
                        if (codEmpresaAnterior != null &&
                            transac.getTsCodEmpresa().equals(codEmpresaAnterior)) {
                            // tmpList.add(transac);
                        } else {
                            // if (codEmpresaAnterior == null) {
                            //     tmpList.add(transac);
                            // }
                            codEmpresaAnterior = transac.getTsCodEmpresa();
                        }
                    }else{
                        RetornoLlamadas r = new RetornoLlamadas();
                        r.setEstado("OK");
                        r.setComentario("CTI_IMAGEN_PASARJDE: Valor N");
                        setEstado(transaction, transac.getCpsId(),r);
                        log.warn("Start Commit Intervalo");
                        // transaction.commit();
                        log.warn("End Commit Intervalo");
                        // Proceso.serviciosJdeAltaImagen.remove(servicio);
                    }
                } else {
                    log.warn("Salteando por intervalo transaccion " + transac.getCpsTransacId());
                    registrarValidacion(transaction,transac.getCpsId(),res.getFechaHayQueProcesar());
                    log.warn("Start Commit Intervalo");
                    // transaction.commit();
                    log.warn("End Commit Intervalo");
                    // Proceso.serviciosJdeAltaImagen.remove(servicio);
                }
            } catch (Exception e) {
                log.warn("Error en JdeAltaImagen: " +  transac.getCpsTransacId() ,e);
            }
            
            // if(tmpList.size() >= Proceso.MAX_POR_LLAMADA) {
            //     break;
            // }
        // }
        
        // if(tmpList.size() > 0) {
            String username = transac.getComEntorno();
            String password = transac.getComPassword();
         
            try {
                /*
                 * Verifica url
                 */
                if(this.url == null) 
                    throw new Exception(Constantes.VERIFICAR_ENDPOINT.getValor() + " " + PARAMETRO_BD);
        
                //Arma input de servicio
                TransaccionPublish in = new TransaccionPublish();
                // for(TablaProcesoServicio servicio : tmpList) {
                    try {
                        TransaccionInternal servicioIn = this.getData(transac.getCpsTransacId());
                        in.getTransaccion().add(servicioIn);
                    } catch (Exception e) {
                        log.warn(e);
                        setException(transac, EstadosLlamadas.ERROR, e);
                        throw e;
                    }
                // }
                
                /*
                 * Parsea request a xml, agrega tag, user y contrase�a
                 */
                String body = this.toXml(in);
                body = "<" + serviceTag + ">" + body + "</" + serviceTag + ">";
                
                body = HeaderImagenAlta.getHeaderRequest2(username, password, body);
                
                //log.info(body);
                
                /*
                 * Llamada a WS
                 */
                SendXML send = new SendXML();
                send.setHostAdress(this.url);
                send.setSoapRequest(body);
                send.call();
        
                /*
                 * Obtiene y parsea respuesta
                 */
                String responseStr = send.getSoapResponse();
                if(responseStr == null) {
                    throw new ServicioException();
                }
                
                log.warn(responseStr);
                response = this.parseResponse(responseStr);  
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
        // } else {
        //     return;
        // }
        
        
        //Elimina elementos procesados
        // Proceso.serviciosJdeAltaImagen.removeAll(tmpList);
    }

    public void fin() throws Throwable {
        // transaction.commit();
    }
    
    /**
     *  Obtiene binario de UCM
     * @param transacId     Id de transaccion
     * @return              TransaccionPublish
     * @throws SQLException
     * @throws Exception
     */
    //TODO ver que se hace con la nueva estrategia del webcenter id y el webcenter id anterior
    private TransaccionInternal getData(Long transacId) throws SQLException, Exception {
        Long webcenterId = DbUtilsCup.getWebcenterId(transaction,transacId);

        Object[] fileUCM = Utils.getUCMFile(transaction, webcenterId);
        FileInfo document = (FileInfo) fileUCM[0];
        byte[] file = (byte[]) fileUCM[1];
                                                  
        if(file == null || ArrayUtils.isEmpty(file)) {
            throw new Exception(Constantes.ERROR_ARCHIVO_NO_ENCONTRADO.getValor() + " para la transaccion " + transacId);
        }
        
        TransaccionInternal ts = new TransaccionInternal();
        ts.setID(transacId);
        ts.setBinario(file);
        ts.setContentType(document.getDDocType());
        ts.setOperacion(Constantes.JDE_ALTA_IMAGEN_OPERACION.getValor());
        
        return ts;
    }
    
    /**
     *  Parsea request
     * @param transaccion   TransaccionPublish
     * @return
     */
    public String toXml(TransaccionPublish transaccion) {
        XStream xstream = new XStream(new DomDriver());
        xstream.autodetectAnnotations(true);
        xstream.alias("", TransaccionPublish.class);
        xstream.alias("transaccion", TransaccionInternal.class);
        
        String xml = xstream.toXML(transaccion);
        
        xml = xml.replace("<>", "");
        xml = xml.replace("</>", "");
        xml = xml.replace(" class=\"com.sun.org.apache.xerces.internal.jaxp.datatype.DateImpl\"", "");        
        return xml;
    }
    
    /**
     *  Parsea response
     * @param response  response
     * @return          ConfirmaAltaImagenPublish
     */
    public ConfirmaAltaImagenPublish parseResponse(String response) {
        String nameSpace = EnvelopeImagenAlta.getNameSpace(response);
        
        XStream xstream = new XStream(new DomDriver());
        xstream.autodetectAnnotations(true);

        xstream.alias(nameSpace + ":Envelope", EnvelopeImagenAlta.class);

        xstream.alias(nameSpace + ":Body", BodyImagenAlta.class);
        xstream.alias(nameSpace + ":Header", HeaderImagenAlta.class);

        xstream.aliasField(nameSpace + ":Body", EnvelopeImagenAlta.class, "body");
        xstream.aliasField(nameSpace + ":Header", EnvelopeImagenAlta.class, "header");
        xstream.aliasField(serviceResponseTag, BodyImagenAlta.class, "estructura");

        xstream.addImplicitCollection(ConfirmaAltaImagenPublish.class, "transaccion");
        xstream.alias(serviceResponseTag, ConfirmaAltaImagenPublish.class);
        xstream.alias("transaccion", ConfirmaAltaTransaccion.class);
        xstream.aliasField("ID", ConfirmaAltaTransaccion.class, "id");

        EnvelopeImagenAlta envelope = (EnvelopeImagenAlta) xstream.fromXML(response);

        return envelope.getBody().getEstructura();
    }
    
    
    public void setResultado(TablaProcesoServicio element, ConfirmaAltaImagenPublish response) throws SQLException {
//        for(int i = 0; i < response.getTransaccion().size(); i++) {
            ConfirmaAltaTransaccion retServicio = response.getTransaccion().get(0);
            TablaProcesoServicio servicioOriginal = element;
            
            RetornoLlamadas r = new RetornoLlamadas();
            r.setEstado(retServicio.getStatus());
            r.setComentario(retServicio.getMensajeError());
            try {
                setEstado(transaction,servicioOriginal.getCpsId(), r);
            } catch (SQLException e) {
                log.warn(e);
                transaction.rollback();
            } finally {
                // transaction.commit();        
            }
            
        }
//    }
 
}
