package com.developer.bigdataindexing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.main.JsonValidator;
import com.github.fge.jsonschema.report.ProcessingReport;
import com.github.fge.jsonschema.util.JsonLoader;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

import redis.clients.jedis.Jedis;

@RestController
@RequestMapping("/")
public class BigDataIndexingController {

	private static Jedis jedis = new Jedis();

	@PostMapping("/")
	public ResponseEntity add(@RequestHeader Map<String, String> headers, @RequestBody String plan) {
		String eTag = null;
		String json = null;

		try {
			JSONObject jObj = new JSONObject(plan);

			String type = jObj.getString("objectType");
			String id = jObj.getString("objectId");
			String product = jedis.get(type + "/" + id);

			String hashedKey = type + "/" + id + "/hashed";
			if (product == null) {
				eTag = eTagGen(plan);
				json = plan;
			} else {
				eTag = eTagGen(product);
				json = "{message: 'Plan already exists!}";
				return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).eTag(eTag).body(json);
			}

			if (jsonValidator(plan)) {
				heirarchy(jObj, product);
				//				jedis.set(type + "/" + id, plan);
				jedis.set(hashedKey, eTag);
			} else {
				throw new ProcessingException("{message : 'JSON validation failed due to missing/incorrect fields!'}");
			}

		} catch (IOException e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("{'message':'" + e.getMessage() + "'}");
		} catch (ProcessingException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("{'message':'" + e.getMessage() + "'}");
		} catch (JSONException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("{'message':'" + e.getMessage() + "'}");
		}

		return ResponseEntity.status(HttpStatus.OK).eTag(eTag).body(json);
	}

	public static Map<String, String> heirarchy(JSONObject objects, String key) {

		HashMap<String, Object> sMap = new Gson().fromJson(objects.toString(), HashMap.class);
		HashMap<String, Map<String, String>> vMap = new HashMap<>();
		Map<String, String> valMap = new HashMap<>();

		for (Map.Entry<String, Object> entry : sMap.entrySet()) {
			String objtype = objects.getString("objectType");
			String objid = objects.getString("objectId");
			if (objtype != null && objid != null) {
				String currKey = objtype + "/" + objid;

				if (entry.getValue() instanceof List) {
					List allItems = (ArrayList) entry.getValue();
					for (int i = 0; i < allItems.size(); i++) {
						// System.out.println(allItems.get(i));
						JSONObject recurObj = new JSONObject(new Gson().toJson(allItems.get(i)));
						heirarchy(recurObj, currKey);
					}
				}
				if (entry.getValue() instanceof Map) {
					// System.out.println("hi");
					JSONObject recurObj = new JSONObject(new Gson().toJson(entry.getValue()));
					heirarchy(recurObj, currKey);
				}

				valMap.put(entry.getKey(), entry.getValue().toString());

				jedis.hmset(currKey, valMap);
			}
		}
		return valMap;
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

		// jedis.hgetAll(type+"/"+id)
	}

	@GetMapping("/")
	public ResponseEntity getAll() {
		return ResponseEntity.status(HttpStatus.OK).body(findAll());
	}

	@GetMapping(value = "/{type}/{id}", headers = "If-Match")
	public ResponseEntity getByIdIfMatch(@RequestHeader("If-Match") String headers, @PathVariable String type,
			@PathVariable String id) {
		JSONObject json = new JSONObject();
		String eTag = null;
		if (type == null || id == null) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body("{'message':'Id and Type can not be empty'}");
		} else {
			String product = jedis.hgetAll(type + "/" + id).toString();
			if (product == null) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body("{'message':'No entry found for provided key, it has either been deleted or updated'}");
			} else {
				// eTag = eTagGen(product);
				try {
					eTag = jedis.get(type + "/" + id + "/hashed");
					if (!headers.equals(eTag)) {
						json = (JSONObject) new JSONParser().parse("{'message': 'Plan provided not already present in database!'}");
						return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).body(json);
					} else {
						json = (JSONObject) new JSONParser().parse(product);					
					}
				} catch (ParseException e) {
					return ResponseEntity.status(HttpStatus.BAD_REQUEST)
							.body("{'message':'Parse Exception: '}"+ e);
				}
			}
			return ResponseEntity.status(HttpStatus.OK).eTag(eTag).body(json);
		}
	}

	@GetMapping(value = "/{type}/{id}", headers = "If-None-Match")
	public ResponseEntity getByIdIfNoneMatch(@RequestHeader("If-None-Match") String headers, @PathVariable String type,
			@PathVariable String id) {
		JSONObject json = new JSONObject();
		String eTag = null;
		if (type == null || id == null) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body("{'message':'Id and Type can not be empty'}");
		} else {
			String product = jedis.hgetAll(type + "/" + id).toString();
			if (product == null) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body("{'message':'No entry found for provided key, it has either been deleted or updated'}");
			} else {
				// eTag = eTagGen(product);
				try {
					eTag = jedis.get(type + "/" + id + "/hashed");
					if (headers.equals(eTag)) {
						json = (JSONObject) new JSONParser().parse("{'message': 'Plan provided is already present in database!'}");

						return ResponseEntity.status(HttpStatus.NOT_MODIFIED).body(json);
					} else {					
						json = (JSONObject) new JSONParser().parse(product);					 
					}
				}catch (ParseException e) {
					return ResponseEntity.status(HttpStatus.BAD_REQUEST)
							.body("{'message':'Parse Exception: '}"+ e);
				}
			}
			return ResponseEntity.status(HttpStatus.OK).eTag(eTag).body(json);
		}
	}

	@GetMapping("/{type}/{id}")
	public ResponseEntity getById(@PathVariable String type, @PathVariable String id){
		JSONObject json = new JSONObject();
		String eTag = null;
		if (type == null || id == null) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body("{'message':'Id and Type can not be empty'}");
		} else {
			String product = jedis.hgetAll(type + "/" + id).toString();
			if (product == null) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body("{'message':'No entry found for provided key, it has either been deleted or updated'}");
			} else {
				// eTag = eTagGen(product);
				eTag = jedis.get(type + "/" + id + "/hashed");
				try {
					json = (JSONObject) new JSONParser().parse(product);
				} catch (ParseException e) {
					return ResponseEntity.status(HttpStatus.BAD_REQUEST)
							.body("{'message':'Parse Exception'}");
				}
			}
			return ResponseEntity.status(HttpStatus.OK).eTag(eTag).body(json);
		}
	}

	@DeleteMapping("/{type}/{id}")
	public ResponseEntity delete(@PathVariable String type, @PathVariable String id) {
		if (type == null || id == null) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body("{'message':'Id and Type can not be empty'}");
		} else {
			String product = jedis.hgetAll(type + "/" + id).toString();
			if (product == null) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body("{'message':'No entry found for provided key, it has either been deleted or updated'}");
			} else {
				jedis.del(type + "/" + id);
				jedis.del(type + "/" + id + "/hashed");
			}
		}
		return ResponseEntity.status(HttpStatus.OK).body("{message: Object deleted successfully!}");
	}

	@ResponseStatus(value = HttpStatus.OK)
	@RequestMapping(method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE, value = "/{type}/{id}")
	public ResponseEntity<String> updatePlan(@RequestBody String item, @PathVariable String type,
			@PathVariable String id, HttpServletRequest request, HttpServletResponse response) throws ProcessingException {

		//check etag
		String if_match = request.getHeader(HttpHeaders.IF_MATCH);
		String planKey = type + "/"+ id;
		if (if_match == null || if_match.isEmpty()) {
			// etag not provided throw 404
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body("{'message':'No entry found for provided key, it has either been deleted or updated'}");
		}

		System.out.println(jedis.hgetAll(planKey));
		if (jedis.hgetAll(planKey).toString() != null && !planKey.equals(if_match)) {
			// hash found in cache but does not match with etag
			return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED)
					.body("{'message':'Plan provided not already present in database!'}");
		}

		Map<String, String> putMap = new HashMap<>();

		//validate resource
		try {
			//			if (jsonValidator(item)) {
			//        	JSONObject plan = (JSONObject) new JSONParser().parse(jedis.get(type+"/"+id));
			JSONObject json = new JSONObject(item);
			String currentKey = json.getString("objectType") + "/"+ json.getString("objectId");
			System.out.println(jedis.hgetAll(planKey).toString());

			System.out.println(json.getString("objectId"));
			System.out.println(jedis.hgetAll(planKey).toString().contains(json.getString("objectId")));
			if(jedis.hgetAll(currentKey).toString()!=null && jedis.hgetAll(planKey).toString().contains(json.getString("objectId"))) {
				putMap = heirarchy(json, currentKey);
			}

			for (Map.Entry<String, String> planEntry : jedis.hgetAll(planKey).entrySet()) {
				System.out.println(planEntry);
				System.out.println(putMap.get("objectId"));
			}
			//			jedis.hmset(, putMap);
			//			}
			//			else {
			//				throw new ProcessingException("{message : 'JSON validation failed due to missing/incorrect fields!'}");
			//			}
		} catch (JSONException e) {
			throw new ProcessingException("{message : 'JSON validation failed due to missing/incorrect fields!'}");
		}  

		return ResponseEntity.status(HttpStatus.OK)
				.body("'message':'Item with type: " + type + " and id: "+ id + " updated successfully!");
	}
}
