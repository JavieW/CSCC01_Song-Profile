package com.csc301.songmicroservice;


import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.result.DeleteResult;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

@Repository
public class SongDalImpl implements SongDal {

	private final MongoTemplate db;

	@Autowired
	public SongDalImpl(MongoTemplate mongoTemplate) {
		this.db = mongoTemplate;
	}

	@Override
	public DbQueryStatus addSong(Song songToAdd) {
		Song song = db.insert(songToAdd);
		DbQueryStatus dbQueryStatus = new DbQueryStatus(null, DbQueryExecResult.QUERY_OK);
		dbQueryStatus.setData(song);
		return dbQueryStatus;
	}

	@Override
	public DbQueryStatus findSongById(String songId) {
		// Initialize the query
		DbQueryStatus dbQueryStatus;
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").is(songId));
		try {
			//find the song
			Song song = db.findOne(query, Song.class);
			// create an appropriate query status
			dbQueryStatus = new DbQueryStatus("Found the Song", DbQueryExecResult.QUERY_OK);
			dbQueryStatus.setData(song);
			return dbQueryStatus;
		} catch (NullPointerException e) {
			// if the result not found
			dbQueryStatus = new DbQueryStatus("Song not found", DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
		} catch (Exception e) {
			// if the server has error
			dbQueryStatus = new DbQueryStatus("Server error", DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
		return dbQueryStatus;
	}

	@Override
	public DbQueryStatus getSongTitleById(String songId) {
		// Initialize the query
		DbQueryStatus dbQueryStatus;
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").is(songId));
		try {
			//find the song
			Song song = db.findOne(query, Song.class);
			// create an appropriate query status
			dbQueryStatus = new DbQueryStatus("Found the name of the song", DbQueryExecResult.QUERY_OK);
			//Extract the name of the song
			dbQueryStatus.setData(song.getSongName());
			return dbQueryStatus;
		} catch (NullPointerException e) {
			// if the result not found
			dbQueryStatus = new DbQueryStatus("Song not found", DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
		} catch (Exception e) {
			// if the server has error
			dbQueryStatus = new DbQueryStatus("Server error", DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
		return dbQueryStatus;
	}

	@Override
	public DbQueryStatus deleteSongById(String songId) {
		DbQueryStatus dbQueryStatus = null;
		//Initialize the query object
		Query query = new Query();
		//Specify query to delete a song by id
		query.addCriteria(Criteria.where("_id").is(songId));
		try {
			// try to remove the song
			DeleteResult deleteResult = db.remove(query, Song.class);
			if (deleteResult.getDeletedCount() == 0) {
				// if the input id does not exist, return not found
				dbQueryStatus = new DbQueryStatus("Id does not exist", DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
			} else {
				dbQueryStatus = new DbQueryStatus("Successfully deleted", DbQueryExecResult.QUERY_OK);
			}
		} catch (Exception e) {
			dbQueryStatus = new DbQueryStatus("Server error", DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
		return dbQueryStatus;
	}

	@Override
	public DbQueryStatus updateSongFavouritesCount(String songId, boolean shouldDecrement) {
		DbQueryStatus dbQueryStatus;
		//Initialize the query and update object
		Query query = new Query();
		Update update = new Update();
		//Specify the query to find the song and change its count
		// if the decrement is true, and the favourite count of the song is greater than 0
		if (shouldDecrement) {
			query.addCriteria(new Criteria().andOperator(Criteria.where("_id").is(songId),
					Criteria.where("songAmountFavourites").gt(0)));
			// decrement the count
			update.inc("songAmountFavourites", -1);
		} else {
			query.addCriteria(Criteria.where("_id").is(songId));
			//increment the count
			update.inc("songAmountFavourites", 1);
		}
		try {
			UpdateResult updateResult = db.updateFirst(query, update, Song.class);
			if (updateResult.getMatchedCount() == 0) {
				dbQueryStatus = new DbQueryStatus("The song is not found or its count is already zero", DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
			} else {
				dbQueryStatus = new DbQueryStatus("The count is updated", DbQueryExecResult.QUERY_OK);
			}
		} catch (Exception e) {
			dbQueryStatus = new DbQueryStatus("Server error", DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
		return dbQueryStatus;
	}
}