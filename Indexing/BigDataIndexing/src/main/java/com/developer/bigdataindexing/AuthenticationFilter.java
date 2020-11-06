package com.developer.bigdataindexing;

import java.io.BufferedReader;
import javax.servlet.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class AuthenticationFilter implements Filter {

    /**
     *
     * @param servletRequest
     * @param servletResponse
     * @param filterChain
     * @throws IOException
     * @throws ServletException
     */
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        
        final String header = request.getHeader("Authorization");
        
        if(header == null || header.isEmpty()){
            errorResponse(response);
            return;
        }

        String token = null;
        token = header.substring(7);

        if (token.isEmpty() || token.trim().equals("")) {
            errorResponse(response);
            return;
        }
        
        InputStream inputSt = getClass().getClassLoader().getResourceAsStream("gsecret.json");
            BufferedReader streamReader = new BufferedReader(new InputStreamReader(inputSt, "UTF-8"));
            JSONObject inputJson = null;
        try {
        	inputJson = (org.json.simple.JSONObject) new JSONParser().parse(streamReader);
        } catch (ParseException ex) {
            Logger.getLogger(AuthenticationFilter.class.getName()).log(Level.SEVERE, null, ex);
        }
        JSONObject jsonData = (JSONObject) inputJson.get("web");
        HttpGet planRequest = new HttpGet(jsonData.get("token_info")+token);
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpResponse planResponse = httpclient.execute(planRequest);
        String responseJSON = EntityUtils.toString(planResponse.getEntity(), Charset.forName("UTF-8"));
        
        if(responseJSON == null || responseJSON.isEmpty() || responseJSON.trim().equals("")){
            errorResponse(response);
            return;            
        }
        
        JSONObject tokenResponseJson = null;
        try {
           tokenResponseJson = (JSONObject) new JSONParser().parse(responseJSON);
        } catch (ParseException ex) {
            Logger.getLogger(AuthenticationFilter.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        if(tokenResponseJson==null){
            errorResponse(response);
            return;
        }
        
        if(tokenResponseJson.get("error") != null){
            errorResponse(response);
            return;
        }
        
        if(!(tokenResponseJson.get("issued_to") != null && tokenResponseJson.get("issued_to").equals(jsonData.get("client_id")))){
            errorResponse(response);
            return;
        }        
 
        filterChain.doFilter(request, response);
    }
    
    public void errorResponse(HttpServletResponse response) throws IOException{

            JSONObject errorJson = new JSONObject();
            errorJson.put("message", "Invalid Token or Token Expired!");
            String Json = errorJson.toString();
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getOutputStream().println(Json);
            response.setContentType("application/json");
            return;
   
    }

}
