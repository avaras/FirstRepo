package com.arcor.cup;

import com.arcor.cup.interfaces.ProcesoServicio;
import com.arcor.cup.service.ProcesoJdeAltas;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import co.elastic.apm.attach.ElasticApmAttacher;

@SpringBootApplication
public class MsJdeAltasApplication implements CommandLineRunner{

	public static void main(String[] args) {
		ElasticApmAttacher.attach();
		SpringApplication.run(MsJdeAltasApplication.class, args);
	}

	@Bean
	public ProcesoServicio getProcesoServicio() {
		return new ProcesoJdeAltas();
	}

	@Override
	public void run(String... args) throws Exception {
		getProcesoServicio().proceso();
	}

}
