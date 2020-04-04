package com.csc301.profilemicroservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.csc301.profilemicroservice.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.*;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/")
public class ProfileController {
	public static final String KEY_USER_NAME = "userName";
	public static final String KEY_USER_FULLNAME = "fullName";
	public static final String KEY_USER_PASSWORD = "password";

	@Autowired
	private final ProfileDriverImpl profileDriver;

	@Autowired
	private final PlaylistDriverImpl playlistDriver;

	OkHttpClient client = new OkHttpClient();

	public ProfileController(ProfileDriverImpl profileDriver, PlaylistDriverImpl playlistDriver) {
		this.profileDriver = profileDriver;
		this.playlistDriver = playlistDriver;
	}

	@RequestMapping(value = "/profile", method = RequestMethod.POST)
	public @ResponseBody Map<String, Object> addSong(@RequestParam Map<String, String> params,
			HttpServletRequest request) {
		// get request body
		String userName = params.get(KEY_USER_NAME);
		String fullName = params.get(KEY_USER_FULLNAME);
		String password = params.get(KEY_USER_PASSWORD);

		// run query
		DbQueryStatus dbQueryStatus = profileDriver.createUserProfile(userName, fullName, password);

		// construct and send response
		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("POST %s", Utils.getUrl(request)));
		response.put("message", dbQueryStatus.getMessage());
		Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), null);
		return response;
	}

	@RequestMapping(value = "/followFriend/{userName}/{friendUserName}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> followFriend(@PathVariable("userName") String userName,
			@PathVariable("friendUserName") String friendUserName, HttpServletRequest request) {

		// run query
		DbQueryStatus dbQueryStatus = profileDriver.followFriend(userName, friendUserName);

		// construct and send response
		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));
		response.put("message", dbQueryStatus.getMessage());
		Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), null);
		return response;
	}

	@RequestMapping(value = "/getAllFriendFavouriteSongTitles/{userName}", method = RequestMethod.GET)
	public @ResponseBody Map<String, Object> getAllFriendFavouriteSongTitles(@PathVariable("userName") String userName,
			HttpServletRequest request) {

		// run query
		DbQueryStatus dbQueryStatus = profileDriver.getAllSongFriendsLike(userName);
		Map<String, List<String>> data = (Map<String, List<String>>) dbQueryStatus.getData();

		// for each friend, replace his songid list with title list
		for (Map.Entry<String, List<String>> entry : data.entrySet()) {
			List<String> songIds = entry.getValue();
			List<String> songTitles = new ArrayList<String>();

			// replace each songId with the corresponding song title
			for (String songId: songIds) {
				try {
					Request getSongByIdRequest = new Request.Builder()
							.url("http://localhost:3001/getSongTitleById/"+songId)
							.get()
							.build();

					String getSongByIdResponse = client.newCall(getSongByIdRequest).execute().body().string();
					int from = getSongByIdResponse.indexOf("\"data\":\"") + ("\"data\":\"").length();
					String title = getSongByIdResponse.substring(from, getSongByIdResponse.indexOf('"', from));
					songTitles.add(title);
				} catch (Exception e) {
					songTitles.add("A song is not found.");
				}
			}
			entry.setValue(songTitles);
		}

		// construct and send response
		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));
		response.put("message", dbQueryStatus.getMessage());
		Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());
		return response;
	}


	@RequestMapping(value = "/unfollowFriend/{userName}/{friendUserName}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> unfollowFriend(@PathVariable("userName") String userName,
			@PathVariable("friendUserName") String friendUserName, HttpServletRequest request) {
		// run query
		DbQueryStatus dbQueryStatus = profileDriver.unfollowFriend(userName, friendUserName);

		// construct and send response
		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));
		response.put("message", dbQueryStatus.getMessage());
		Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), null);
		return response;
	}

	@RequestMapping(value = "/likeSong/{userName}/{songId}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> likeSong(@PathVariable("userName") String userName,
			@PathVariable("songId") String songId, HttpServletRequest request) {

		// initialize response
		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));

		try {
			// check the existence of the song in MongoDB
			Request getSongByIdRequest = new Request.Builder()
					.url("http://localhost:3001/getSongById/"+songId)
					.get()
					.build();

			String getSongByIdResponse = client.newCall(getSongByIdRequest).execute().body().string();
			if (!getSongByIdResponse.contains("\"status\":\"OK\"")) {
				response.put("message", "The song is not found!");
				Utils.setResponseStatus(response, DbQueryExecResult.QUERY_ERROR_NOT_FOUND, null);
				return response;
			}
			// check whether or not the song is already liked by the user in Neo4j
			DbQueryStatus dbQueryStatus = playlistDriver.likeSong(userName, songId);

			// increment the Favourites in MongoDB if this is the first time the user like the song
			if (dbQueryStatus.getdbQueryExecResult().equals(DbQueryExecResult.QUERY_OK) && (Boolean)dbQueryStatus.getData()) {
				Request mongoRequest = new Request.Builder()
						.url("http://localhost:3001/updateSongFavouritesCount/"+songId+"?shouldDecrement=false")
						.put(Utils.emptyRequestBody)
						.build();
				client.newCall(mongoRequest).execute();
			}

			// send response
			response.put("message", dbQueryStatus.getMessage());
			Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), null);
			return response;

		} catch (Exception e) {
			e.printStackTrace();
			response.put("message", "Internal Error");
			Utils.setResponseStatus(response, DbQueryExecResult.QUERY_ERROR_GENERIC, null);
			return response;
		}
	}

	@RequestMapping(value = "/unlikeSong/{userName}/{songId}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> unlikeSong(@PathVariable("userName") String userName,
			@PathVariable("songId") String songId, HttpServletRequest request) {
		// initialize response
		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));

		try {
			// check whether or not the user successfully remove the song from his favorites list
			DbQueryStatus dbQueryStatus = playlistDriver.unlikeSong(userName, songId);

			// if so, decrement the favorites count
			if (dbQueryStatus.getdbQueryExecResult().equals(DbQueryExecResult.QUERY_OK)) {
				Request mongoRequest = new Request.Builder()
						.url("http://localhost:3001/updateSongFavouritesCount/"+songId+"?shouldDecrement=true")
						.put(Utils.emptyRequestBody)
						.build();
				client.newCall(mongoRequest).execute();
			}

			response.put("message", dbQueryStatus.getMessage());
			Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), null);
			return response;

		} catch (Exception e) {
			e.printStackTrace();
			response.put("message", "Internal Error");
			Utils.setResponseStatus(response, DbQueryExecResult.QUERY_ERROR_GENERIC, null);
			return response;
		}
	}

	@RequestMapping(value = "/deleteAllSongsFromDb/{songId}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> deleteAllSongsFromDb(@PathVariable("songId") String songId,
			HttpServletRequest request) {

		// run query
		DbQueryStatus dbQueryStatus = playlistDriver.deleteSongFromDb(songId);

		// construct and send response
		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));
		response.put("message", dbQueryStatus.getMessage());
		Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), null);
		return response;
	}
}