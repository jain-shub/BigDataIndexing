package com.developer.bigdataindexing;


//import org.json.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class PlanService {

	private JedisPool jedisPool;

	private JedisPool getJedisPool() {
		if (this.jedisPool == null) {
			this.jedisPool = new JedisPool();
		}
		return this.jedisPool;
	}

	public String savePlan(JSONObject jObj, String objectType) {
		// temp array of keys to remove from json object
		ArrayList<String> keysToDelete = new ArrayList<String>();

		// Iterate through the json
		for(Object key : jObj.keySet()) {
			// check if the value of key is JSONObject or JSONArray
			// first get current Value
			Object currentValue = jObj.get(key.toString());
			if(currentValue instanceof JSONObject) {
				String objectKey = this.savePlan((JSONObject)currentValue, key.toString());
				// remove this value from JSON, as it will be stored separately
				keysToDelete.add(key.toString());

				// save the relation as separate key in redis
				Jedis jedis = this.getJedisPool().getResource();
				String relationKey = objectType + "_" + jObj.get("objectId") + "_" + key.toString();
				jedis.set(relationKey, objectKey);
				jedis.close();

			} else if (currentValue instanceof JSONArray) {
				JSONArray currentArrayValue = (JSONArray)currentValue;
				//temp array to store keys of individual objects
				String[] tempValues = new String[currentArrayValue.size()];

				//iterate through the array
				for (int i = 0; i < currentArrayValue.size(); i++) {
					if (currentArrayValue.get(i) instanceof JSONObject) {
						JSONObject arrayObject = (JSONObject)currentArrayValue.get(i);
						String arrayObjectKey = this.savePlan(arrayObject, (String)arrayObject.get("objectType"));

						tempValues[i] = arrayObjectKey;
					}
				}

				keysToDelete.add(key.toString());

				// save the Array as separate key in redis
				Jedis jedis = this.getJedisPool().getResource();
				String relationKey = objectType + "_" + jObj.get("objectId") + "_" + key.toString();
				jedis.set(relationKey, Arrays.toString(tempValues));
				jedis.close();

			}
		}

		// Remove objects from json that are stored separately
		//		        for (String key : keysToDelete) {
		//		            jObj.remove(key);
		//		        }

		//save the current object in redis
		String objectKey = objectType + "_" + jObj.get("objectId");

		Jedis jedis = this.getJedisPool().getResource();
		jedis.set(objectKey, jObj.toString());
		jedis.close();

		return objectKey;
	}

	public JSONObject getPlan(String planKey) throws ParseException {
		JedisPool jedisPool = new JedisPool();
		Jedis jedis;
		JSONObject json;
		if (isStringArray(planKey)) {
			ArrayList<JSONObject> arrayValue = getFromArrayString(planKey);
			Gson gson = new Gson();
			String items = gson.toJson(arrayValue);

			json = (JSONObject) new JSONParser().parse(items);
		} else {
			jedis = jedisPool.getResource();
			String jsonString = jedis.get(planKey);
			jedis.close();
			if (jsonString == null || jsonString.isEmpty()) {
				return null;
			}
			json = (JSONObject) new JSONParser().parse(jsonString);
		}

		// fetch additional relations for the object, if present
		jedis = jedisPool.getResource();
		Set<String> jsonRelatedKeys = jedis.keys(planKey + "_*");
		jedis.close();

		Iterator<String> keysIterator = jsonRelatedKeys.iterator();
		while(keysIterator.hasNext()) {
			String partObjKey = keysIterator.next();
			String partObjectKey = partObjKey.substring(partObjKey.lastIndexOf('_')+1);

			// fetch the id stored at partObjKey
			jedis = jedisPool.getResource();
			String partObjectDBKey = jedis.get(partObjKey);
			jedis.close();
			if (partObjectDBKey == null || partObjectDBKey.isEmpty()) {
				continue;
			}

			if(isStringArray(partObjectDBKey)) {
				ArrayList<JSONObject> arrayValue = getFromArrayString(partObjectDBKey);
				json.put(partObjectKey, arrayValue);
			} else {
				JSONObject partObj = this.getPlan(partObjectDBKey);
				//add partObj to original object
				json.put(partObjectKey, partObj);
			}
		}

		return json;
	}

	public boolean deletePlan(String planKey) {

		JedisPool jedisPool = new JedisPool();
		Jedis jedis;

		if(isStringArray(planKey)) {
			// delete all keys in the array
			String[] arrayKeys = planKey.substring(planKey.indexOf("[")+1, planKey.lastIndexOf("]")).split(", ");
			for (String key : arrayKeys) {
				if(!this.deletePlan(key)) {
					//deletion failed
					return false;
				}
			}
		} else{
			jedis = jedisPool.getResource();
			if(jedis.del(planKey) < 1) {
				// deletion failed
				jedis.close();
				return false;
			}
			jedis.close();
		}

		// fetch additional relations for the object, if present
		jedis = jedisPool.getResource();
		Set<String> jsonRelatedKeys = jedis.keys(planKey + "_*");
		jedis.close();

		Iterator<String> keysIterator = jsonRelatedKeys.iterator();
		while(keysIterator.hasNext()) {
			String partObjKey = keysIterator.next();
			String partObjectKey = partObjKey.substring(partObjKey.lastIndexOf('_')+1);

			// fetch the id stored at partObjKey
			jedis = jedisPool.getResource();
			String partObjectDBKey = jedis.get(partObjKey);
			if(jedis.del(partObjKey) < 1) {
				//deletion failed
				return false;
			}
			jedis.close();
			if (partObjectDBKey == null || partObjectDBKey.isEmpty()) {
				continue;
			}

			if(isStringArray(partObjectDBKey)) {
				// delete all keys in the array
				String[] arrayKeys = partObjectDBKey.substring(partObjectDBKey.indexOf("[")+1, partObjectDBKey.lastIndexOf("]")).split(", ");
				for (String key : arrayKeys) {
					if(!this.deletePlan(key)) {
						//deletion failed
						return false;
					}
				}
			} else {
				if(!this.deletePlan(partObjectDBKey)){
					//deletion failed
					return false;
				}
			}
		}


		return true;
	}

	public String updatePlan(JSONObject json, String objectType) {
		String objectId;

		//delete the object
		if(!this.deletePlan(objectType + "_" + (String)json.get("objectId"))) {
			// planKey not found
			return null;
		}

		//save the updated object
		objectId = this.savePlan(json, objectType);

		return objectId;
	}

	// merge the incoming json object with the object in db.
	public JSONObject mergeJson(JSONObject json, String objectKey) throws ParseException {
		JSONObject savedObject = this.getPlan(objectKey);
		if (savedObject == null)
			return null;

		// iterate the new json object
		for(Object jsonKey : json.keySet()) {
			Object jsonValue = json.get(jsonKey.toString());

			// check if this is an existing object
			if (savedObject.get(jsonKey.toString()) == null) {
				savedObject.put(jsonKey.toString(), jsonValue);
			} else {
				if (jsonValue instanceof JSONObject) {
					JSONObject jsonValueObject = (JSONObject)jsonValue;
					String jsonObjKey = jsonKey + "_" + jsonValueObject.get("objectId");
					if (((JSONObject)savedObject.get(jsonKey.toString())).get("objectId").equals(jsonValueObject.get("objectId"))) {
						savedObject.put(jsonKey.toString(), jsonValue);
					} else {
						JSONObject updatedJsonValue = this.mergeJson(jsonValueObject, jsonObjKey);
						savedObject.put(jsonKey.toString(), updatedJsonValue);
					}
				} else if (jsonValue instanceof JSONArray) {
					JSONArray jsonValueArray = (JSONArray) jsonValue;
					JSONArray savedJSONArray = (JSONArray) new JSONParser().parse(JSONValue.toJSONString(savedObject.get(jsonKey.toString())));
					//                    JSONArray savedJSONArray = (JSONArray) savedObject.get(jsonKey.toString());
					for (int i = 0; i < jsonValueArray.size(); i++) {
						JSONObject arrayItem = (JSONObject)jsonValueArray.get(i);
						//check if objectId already exists in savedJSONArray
						int index = getIndexOfObjectId(savedJSONArray, (String)arrayItem.get("objectId"));
						if(index >= 0) {
							savedJSONArray.remove(index);
						}
						savedJSONArray.add(arrayItem);
					}
					savedObject.put(jsonKey.toString(), savedJSONArray);
				} else {
					savedObject.put(jsonKey.toString(), jsonValue);
				}
			}

		}

		return savedObject;
	}



	private boolean isStringArray(String str) {
		if (str.indexOf('[') < str.indexOf(']')) {
			if (str.substring((str.indexOf('[') + 1), str.indexOf(']')).split(", ").length > 0)
				return true;
			else
				return false;
		} else {
			return false;
		}
	}

	private ArrayList<JSONObject> getFromArrayString(String keyArray) throws ParseException {
		ArrayList<JSONObject> jsonArray = new ArrayList<>();
		String[] array = keyArray.substring((keyArray.indexOf('[') + 1), keyArray.indexOf(']')).split(", ");

		for (String key : array) {
			JSONObject partObj = this.getPlan(key);
			jsonArray.add(partObj);
		}

		return jsonArray;
	}

	private int getIndexOfObjectId(JSONArray array, String objectId) {
		for (int i = 0; i < array.size(); i++) {
			JSONObject arrayObj = (JSONObject)array.get(i);
			String itemId = (String)arrayObj.get("objectId");
			if (objectId.equals(itemId)){
				return i;
			}
		}

		return -1;
	}
}
