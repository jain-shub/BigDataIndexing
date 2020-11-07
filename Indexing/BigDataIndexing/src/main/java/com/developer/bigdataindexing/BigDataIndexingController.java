package com.developer.bigdataindexing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.main.JsonValidator;
import com.github.fge.jsonschema.report.ProcessingReport;
import com.github.fge.jsonschema.util.JsonLoader;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@RestController
@RequestMapping("/")
public class BigDataIndexingController {

	private static Jedis jedis = new Jedis();
	PlanService planService = new PlanService();
	Map<String, String> cacheMap = new HashMap<String, String>();

	@PostMapping("/plan")
	public ResponseEntity add(@RequestHeader Map<String, String> headers, @RequestBody String plan,  HttpServletRequest request, HttpServletResponse response) throws ParseException {
		String eTag = null;

		JSONObject jObj = (JSONObject) new JSONParser().parse(plan);

		try {
			String type = (String) jObj.get("objectType");
			String id = (String) jObj.get("objectId");
			String product = jedis.get(type + "/" + id);

			String hashedKey = type + "/" + id + "/hashed";
			if (product == null) {
				jObj = (JSONObject) new JSONParser().parse(plan);
			} else {
				jObj = (JSONObject) new JSONParser().parse("{\"message\":\"Plan already exists!\"}");
				return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(jObj);
			}

			if (!jsonValidator(plan)) {
				jObj = (JSONObject) new JSONParser().parse("{\"message\":\"JSON validation failed due to missing/incorrect fields!\"}");
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(jObj);
			} 
			String objectKey = this.planService.savePlan(jObj, (String)jObj.get("objectType"));
			this.cacheMap.put(objectKey, eTagGen(jObj.toString()));
			System.out.println(this.cacheMap.get(objectKey));
			System.out.println(this.cacheMap.toString());

		} catch (IOException e) {
			jObj = (JSONObject) new JSONParser().parse("{\"message\": " + e.getMessage()+ "\"}");
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(jObj);
		} catch (ProcessingException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(jObj);
		} catch (JSONException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(jObj);
		}

		response.setHeader(HttpHeaders.ETAG, eTagGen(jObj.toString()));
		return ResponseEntity.status(HttpStatus.OK).body(jObj);
	}

	public static String readFileAsString(String file) throws IOException {
		return new String(Files.readAllBytes(Paths.get(file)));
	}

	public String eTagGen(String plan) {
		String eTag = new DigestUtils("SHA3-256").digestAsHex(plan);
		return eTag;
	}

	public static boolean jsonValidator(String plan) throws IOException, ProcessingException {
		String file = "src/main/resources/schema.json";
		String jsonFile = readFileAsString(file);
		JsonNode data = JsonLoader.fromString(plan);
		JsonNode schema = JsonLoader.fromString(jsonFile);
		final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
		JsonValidator validator = factory.getValidator();
		ProcessingReport report = validator.validate(schema, data);
		if (report.isSuccess()) {
			return true;
		}
		return false;
	}

	public Set<Map<String, String>> findAll() {
		Set<String> sets = jedis.keys("*");
		Set<Map<String, String>> resultSet = new HashSet<>();
		if (!CollectionUtils.isEmpty(sets)) {
			for (String key : sets) {
				Map<String, String> result;
				result = jedis.hgetAll(key);
				resultSet.add(result);
			}
		}
		return resultSet;
	}

	@GetMapping("/plan")
	public ResponseEntity getAll() {
		return ResponseEntity.status(HttpStatus.OK).body(findAll());
	}

	@ResponseStatus(value = HttpStatus.OK)
	@RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
	public ResponseEntity getById(@PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) throws ParseException{
		
		JSONObject retJson = new JSONObject();
		String if_none_match = request.getHeader(HttpHeaders.IF_NONE_MATCH);
		String objKey = "plan_" + objectId;
		if (this.cacheMap.get(objKey) != null && this.cacheMap.get(objKey).equals(if_none_match)) {
			// etag matches, send 304
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"Entry not modified\"}");
			return ResponseEntity.status(HttpStatus.NOT_MODIFIED).body(retJson);
		}

		JedisPool jedisPool = new JedisPool();
		Jedis jedis = jedisPool.getResource();
		JSONObject json = this.planService.getPlan(objKey);
		if (json == null) {
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"No entry found for provided key, it has either been deleted or updated\"}");
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(retJson);
		}

		this.cacheMap.put(objKey, String.valueOf(eTagGen(json.toString())));
//		response.setHeader(HttpHeaders.ETAG, String.valueOf(json.hashCode()));
		response.setHeader(HttpHeaders.ETAG, eTagGen(json.toString()));

		return ResponseEntity.ok().body(json);
	}

	@ResponseStatus(value = HttpStatus.OK)
	@RequestMapping(method = RequestMethod.PATCH, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
	public ResponseEntity patchPlan(@RequestBody String json, @PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) throws ParseException, IOException, ProcessingException {

		JSONObject retJson = new JSONObject();
		//check etag
		String if_match = request.getHeader(HttpHeaders.IF_MATCH);
		if (if_match == null || if_match.isEmpty()) {
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"No entry found for provided key, it has either been deleted or updated\"}");
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(retJson);
		}
		
		System.out.println(this.cacheMap.get("plan_"+objectId));
		if (this.cacheMap.get("plan_"+objectId) != null && !this.cacheMap.get("plan_"+objectId).equals(if_match)) { 
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"Plan provided not already present!\"}");
			return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).body(retJson);
		}

		JSONObject plan = (JSONObject) new JSONParser().parse(json);

		if (!jsonValidator(json)) {
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"JSON validation failed due to missing/incorrect fields!\"}");
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(retJson);
		} 

		//merge json with saved value
		JSONObject mergedJson = this.planService.mergeJson(plan, "plan_"+objectId);
		if (mergedJson == null) {
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"No entry found for provided key, it has either been deleted or updated\"}");
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(retJson);
		}



		JSONObject planToIndex = mergedJson;

		//update json
		String objectKey = this.planService.updatePlan(mergedJson, (String)mergedJson.get("objectType"));
		if (objectKey == null) {
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"Update Failed\"}");
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(retJson);
		}

		this.cacheMap.put(objectKey, eTagGen(mergedJson.toString()));
		response.setHeader(HttpHeaders.ETAG, eTagGen(mergedJson.toString()));

		return new ResponseEntity(HttpStatus.NO_CONTENT);
	}

	@DeleteMapping("/plan/{id}")
	public ResponseEntity delete(@PathVariable String id, HttpServletRequest request, HttpServletResponse response) throws ParseException {
		JSONObject retJson = new JSONObject();
		String if_match = request.getHeader(HttpHeaders.IF_MATCH);
        if (if_match == null || if_match.isEmpty()) {
        	retJson = (JSONObject) new JSONParser().parse("{\"message\":\"No entry found for provided key, it has either been deleted or updated\"}");
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(retJson);
        }
        if (this.cacheMap.get("plan_" + id) != null && !this.cacheMap.get("plan_" + id).equals(if_match)) {
            // hash found in cache but does not match with etag
        	retJson = (JSONObject) new JSONParser().parse("{\"message\":\"Plan provided not already present!\"}");
			return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).body(retJson);
        } else {
            if (!this.planService.deletePlan("plan_" + id)) {
            	retJson = (JSONObject) new JSONParser().parse("{\"message\":\"No entry found for provided key, it has either been deleted or updated\"}");
    			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(retJson);
            }         

            //delete the cache
            this.cacheMap.remove("plan_" + id);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        }
	}

	@ResponseStatus(value = HttpStatus.OK)
	@RequestMapping(method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{id}")
	public ResponseEntity updatePlan(@RequestBody String item, @PathVariable String id, HttpServletRequest request, HttpServletResponse response) throws ProcessingException, ParseException, IOException {

		JSONObject retJson = new JSONObject();
		//check etag
		String if_match = request.getHeader(HttpHeaders.IF_MATCH);
		System.out.println(if_match);
		//		String planKey = type + "/"+ id;
		if (if_match == null || if_match.isEmpty()) {
			// etag not provided throw 404
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"No entry found for provided key, it has either been deleted or updated\"}");
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(retJson);
		}

		if (this.cacheMap.get("plan_" + id) != null && !this.cacheMap.get("plan_" + id).equals(if_match)) {
			// hash found in cache but does not match with etag
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"Plan provided not already present!\"}");
			return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED)
					.body(retJson);
		}

		if (!jsonValidator(item)) {
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"JSON validation failed due to missing/incorrect fields!\"}");
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(retJson);
		} 

		JSONObject plan = (JSONObject) new JSONParser().parse(item);
		//validate resource
		try {
			String objectKey = this.planService.updatePlan(plan, (String)plan.get("objectType"));
			//update etag
			this.cacheMap.put(objectKey, eTagGen(plan.toString()));			
		} catch (JSONException e) {
			retJson = (JSONObject) new JSONParser().parse("{\"message\":\"JSON validation failed due to missing/incorrect fields!\"}");
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(retJson);
		}  


		response.setHeader(HttpHeaders.ETAG, eTagGen(plan.toString()));
//		response.setHeader(HttpHeaders.ETAG, String.valueOf(plan.hashCode()));
		retJson = (JSONObject) new JSONParser().parse("{\"message\":\"Item has been updated with id:" + id + "\"}");
		return ResponseEntity.status(HttpStatus.OK)
				.body(retJson);
	}
}
