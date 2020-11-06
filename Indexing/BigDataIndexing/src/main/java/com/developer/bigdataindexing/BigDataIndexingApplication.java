package com.developer.bigdataindexing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;



@SpringBootApplication
public class BigDataIndexingApplication {

	public static void main(String[] args) {
		SpringApplication.run(BigDataIndexingApplication.class, args);
	}
	
	@Bean
    public FilterRegistrationBean<AuthenticationFilter> filterRegistrationBean() {

        System.out.println("com.example.demo.configuration.AppConfig.filterRegistrationBean()");
        System.out.println("APP CONFIG JAVA ");
        FilterRegistrationBean<AuthenticationFilter> registrationBean = new FilterRegistrationBean();
        AuthenticationFilter authFilter = new AuthenticationFilter();

        registrationBean.setFilter(authFilter);
        registrationBean.addUrlPatterns("*");
        return registrationBean;
    }

}
