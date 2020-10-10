package com.developer.bigdataindexing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.main.JsonValidator;
import com.github.fge.jsonschema.report.ProcessingReport;
import com.github.fge.jsonschema.util.JsonLoader;

import redis.clients.jedis.Jedis;

@RestController
@RequestMapping("/")
public class BigDataIndexingController {


	private Jedis jedis = new Jedis();

	@PostMapping("/")
	public ResponseEntity add(@RequestHeader Map<String, String> headers, @RequestBody String plan){
		String eTag = null;
		String json = null;

		try {
			JSONObject jObj = new JSONObject(plan);
			String type = jObj.getString("objectType");
			String id = jObj.getString("objectId");
			String product = jedis.get(type+"/"+id);
			String hashedKey = type+"/"+id+"/hashed";
			if(product==null) {
				eTag = eTagGen(plan);
				json = plan;
			}
			else {
				eTag = eTagGen(product);
				json = "{message: 'Plan already exists!}";
				return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
						.eTag(eTag)
						.body(json);
			}

			if(jsonValidator(plan)) {
				jedis.set(type+"/"+id, plan);
				jedis.set(hashedKey, eTag);
			} else {
				throw new ProcessingException("{message : 'JSON validation failed due to missing/incorrect fields!'}");
			}

		} catch (IOException e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).
					body("{'message':'"+ e.getMessage() +"'}");
		} catch (ProcessingException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).
					body("{'message':'"+ e.getMessage() +"'}");
		} catch (JSONException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).
					body("{'message':'"+ e.getMessage() +"'}");
		}

		return ResponseEntity.status(HttpStatus.OK)
				.eTag(eTag)
				.body(json);
	}

	public static String readFileAsString(String file) throws IOException
	{
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
		if(report.isSuccess()) {			
			return true;
		}
		return false;
	}

	public Set<String> findAll(){
		Set<String> sets = jedis.keys("*");
		Set<String> resultSet = new HashSet<>(); 
		if (!CollectionUtils.isEmpty(sets)) {
			for (String key : sets) {
				String result;
				result = jedis.get(key);
				resultSet.add(result);
			}
		}
		return resultSet;
	}

	@GetMapping("/")
	public ResponseEntity getAll() {
		return ResponseEntity.status(HttpStatus.OK).body(findAll());
	}

	@GetMapping(value = "/{type}/{id}", headers = "If-Match")
	public ResponseEntity getByIdIfMatch(@RequestHeader("If-Match") String headers, @PathVariable String type, @PathVariable String id) {
		String json = null;
		String eTag = null;
		if(type==null || id == null) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
					.body("{'message':'Id and Type can not be empty'}");
		}
		else {
			String product = jedis.get(type+"/"+id);
			if(product==null) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body("{'message':'No entry found for provided key, it has either been deleted or updated'}");
			}
			else {
//				eTag = eTagGen(product);
				eTag = jedis.get(type+"/"+id+"/hashed");
				if(!headers.equals(eTag)) {
					json = "{'message': 'Plan provided not already present in database!'}";
					return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED)
							.body(json);
				}
				else {
					json = product;
				}
			}
			return ResponseEntity.status(HttpStatus.OK)
					.eTag(eTag)
					.body(json);
		}
	}

	@GetMapping(value = "/{type}/{id}", headers = "If-None-Match")
	public ResponseEntity getByIdIfNoneMatch(@RequestHeader("If-None-Match") String headers, @PathVariable String type, @PathVariable String id) {
		String json = null;
		String eTag = null;
		if(type==null || id == null) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
					.body("{'message':'Id and Type can not be empty'}");
		}
		else {
			String product = jedis.get(type+"/"+id);
			if(product==null) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body("{'message':'No entry found for provided key, it has either been deleted or updated'}");
			}
			else {
//				eTag = eTagGen(product);	
				eTag = jedis.get(type+"/"+id+"/hashed");
				if(headers.equals(eTag)) {					
					json = "{'message': 'Plan provided is already present in database!'}";

					return ResponseEntity.status(HttpStatus.NOT_MODIFIED)
							.body(json);
				}
				else {
					json = product;
				}
			}
			return ResponseEntity.status(HttpStatus.OK)
					.eTag(eTag)
					.body(json);
		}
	}

	@GetMapping("/{type}/{id}")
	public ResponseEntity getById(@PathVariable String type, @PathVariable String id) {
		String json = null;
		String eTag = null;
		if(type==null || id == null) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
					.body("{'message':'Id and Type can not be empty'}");
		}
		else {
			String product = jedis.get(type+"/"+id);
			if(product==null) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body("{'message':'No entry found for provided key, it has either been deleted or updated'}");
			}
			else {
//				eTag = eTagGen(product);	
				eTag = jedis.get(type+"/"+id+"/hashed");
				json = product;			
			}
			return ResponseEntity.status(HttpStatus.OK)
					.eTag(eTag)
					.body(json);
		}
	}

	@DeleteMapping("/{type}/{id}")
	public ResponseEntity delete(@PathVariable String type, @PathVariable String id) {
		if(type==null || id == null) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
					.body("{'message':'Id and Type can not be empty'}");
		}
		else {
			String product = jedis.get(type+"/"+id);
			if(product==null) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body("{'message':'No entry found for provided key, it has either been deleted or updated'}");
			}
			else {
				jedis.del(type+"/"+id);
				jedis.del(type+"/"+id+"/hashed");
			}			
		}
		return ResponseEntity.status(HttpStatus.OK).body("{message: Object deleted successfully!}");
	}
}
