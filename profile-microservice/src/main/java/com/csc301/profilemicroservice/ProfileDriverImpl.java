package com.csc301.profilemicroservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import org.springframework.stereotype.Repository;
import org.neo4j.driver.v1.Transaction;

import static org.neo4j.driver.v1.Values.parameters;

@Repository
public class ProfileDriverImpl implements ProfileDriver {

	Driver driver = ProfileMicroserviceApplication.driver;

	public static void InitProfileDb() {
		String queryStr;

		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				queryStr = "CREATE CONSTRAINT ON (nProfile:profile) ASSERT exists(nProfile.userName)";
				trans.run(queryStr);

				queryStr = "CREATE CONSTRAINT ON (nProfile:profile) ASSERT exists(nProfile.password)";
				trans.run(queryStr);

				queryStr = "CREATE CONSTRAINT ON (nProfile:profile) ASSERT nProfile.userName IS UNIQUE";
				trans.run(queryStr);

				trans.success();
			}
			session.close();
		}
	}
	
	@Override
	public DbQueryStatus createUserProfile(String userName, String fullName, String password) {

		try (Session session = driver.session()) {

			try (Transaction trans = session.beginTransaction()) {
				trans.run("CREATE (:profile {userName: $userName, name: $fullName, " +
								"password: $password}) -[:created]-> (:playlist {plName: $plName})",
						parameters( "userName", userName, "fullName", fullName,
								"password", password, "plName", (userName+"-favorites")));
				trans.success();
			}
			session.close();

			return new DbQueryStatus("Successfully created profile.", DbQueryExecResult.QUERY_OK);
		} catch (Exception e) {
			e.printStackTrace();
			return new DbQueryStatus("UserName already exist!", DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
	}

	@Override
	public DbQueryStatus followFriend(String userName, String frndUserName) {

		try (Session session = driver.session()) {

			StatementResult statementResult;
			String message;

			// check self follow
			if (userName.equals(frndUserName)) {
				message = "User " + userName + " cannot follow himself!";
				return new DbQueryStatus(message, DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
			}

			// run query
			try (Transaction trans = session.beginTransaction()) {
				statementResult = trans.run("match (p:profile {userName: $userName})" +
								", (f:profile {userName: $frndUserName}) merge ((p) -[r:follows]->(f)) return r",
						parameters( "userName", userName, "frndUserName", frndUserName));
				trans.success();
			}
			session.close();

			// return proper DbQueryStatus
			if (statementResult.hasNext()) {
				message = "User " + userName + " successfully followed User " + frndUserName;
				return new DbQueryStatus(message, DbQueryExecResult.QUERY_OK);
			} else {
				message = "User " + userName + " or User " + frndUserName + " not found";
				return new DbQueryStatus(message, DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
			}

		} catch (Exception e) {
			e.printStackTrace();
			return new DbQueryStatus("Internal Error", DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
	}

	@Override
	public DbQueryStatus unfollowFriend(String userName, String frndUserName) {

		try (Session session = driver.session()) {
			StatementResult statementResult = null;
			DbQueryStatus dbQueryStatus;
			String message;

			// check whether or not two users are friends
			try (Transaction trans = session.beginTransaction()) {
				statementResult = trans.run("match (p:profile {userName: $userName}) -[r:follows]-> " +
								"(f:profile {userName: $frndUserName}) return r",
						parameters( "userName", userName, "frndUserName", frndUserName));

				// if they are friends
				if (statementResult.hasNext()) {
			 		trans.run("match (p:profile {userName: $userName}) -[r:follows]-> " +
							"(f:profile {userName: $frndUserName}) delete r",
							parameters( "userName", userName, "frndUserName", frndUserName));
					trans.success();
					message = "User " + userName + " successfully unfollowed User " + frndUserName;
					dbQueryStatus =  new DbQueryStatus(message, DbQueryExecResult.QUERY_OK);
				} else {	// otherwise
					message = "User " + userName + " and User " + frndUserName + " are not friends";
					dbQueryStatus =  new DbQueryStatus(message, DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
				}
			}

			session.close();
			return dbQueryStatus;
		} catch (Exception e) {
			e.printStackTrace();
			return new DbQueryStatus("Internal Error", DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
	}

	@Override
	public DbQueryStatus getAllSongFriendsLike(String userName) {

		try (Session session = driver.session()) {
			StatementResult frndPlResult;
			StatementResult frndSongResult;
			DbQueryStatus dbQueryStatus;
			String message;
			String frndPl;
			Map<String, List<String>> data = new HashMap<>();

			try (Transaction trans = session.beginTransaction()) {
				// get all friends of User
				frndPlResult = trans.run("match (:profile{userName: $userName})-[:follows]->(f)" +
								"match (f) -[:created]-> (pl) return pl.plName",
						parameters( "userName", userName));

				// for each friend get all his songs
				while (frndPlResult.hasNext()) {
					frndPl = frndPlResult.next().get(0).toString().replaceAll("\"", "");
					frndSongResult = trans.run("match (:playlist{plName: $plName})" +
									"-[:includes]->(s) return s.songId",
							parameters("plName", frndPl));

					List<String> songs = new ArrayList<String>();
					while (frndSongResult.hasNext()){
						songs.add(frndSongResult.next().get(0).asString().replaceAll("\"", ""));
					}
					data.put(frndPl.substring(0, frndPl.indexOf("-favorites")), songs);
				}
				trans.success();
			}
			session.close();
			message = "Successfully got all songs the user's friends like.";
			dbQueryStatus = new DbQueryStatus(message, DbQueryExecResult.QUERY_OK);
			dbQueryStatus.setData(data);
			return dbQueryStatus;

		} catch (Exception e) {
			e.printStackTrace();
			return new DbQueryStatus("Internal Error", DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
	}

}
