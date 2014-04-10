package org.cloudydemo;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.Lock;
import javax.ejb.LockType;

import org.bson.types.ObjectId;
import org.cloudydemo.model.Application;
import org.cloudydemo.model.Gear;
import org.cloudydemo.model.Hit;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;

@Startup
@Singleton
public class HitTracker {
	private Mongo mongo;
	private DB mongoDB;

	private static final Logger LOGGER = Logger.getLogger(HitTracker.class
			.getName());

	// Cached number of hits
	private int hits;

	// The gear id of the instance this singleton is running on
	private String gearId;

	// The application name
	private String appName;

	private final String COLLECTION = "hitTracker";

	@PostConstruct
	void initialize() {
		gearId = System.getenv("HOSTNAME");
		appName = "scaling-demo";


		String user = null;
		String password = null;
		try {
			List addrs = new ArrayList();
 			// addrs.add( new ServerAddress("192.168.1.1" , 27017));
 			addrs.add( new ServerAddress("192.168.1.2" , 27017));
 			// addrs.add( new ServerAddress("192.168.1.3" , 27017));
 			// addrs.add( new ServerAddress("192.168.1.4" , 27018));

 			LOGGER.fine("Connecting with " + addrs.toString());
 			
			mongo = new Mongo(addrs);
			// mongo.slaveOk();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		mongoDB = mongo.getDB(appName);
		if (user != null && password != null) {
			if (mongoDB.authenticate(user, password.toCharArray()) == false) {
				throw new RuntimeException("Mongo authentication failed");
			}
		} else {
			LOGGER.warning("No username / password given so not authenticating with Mongo");
		}
	}

    @Lock(LockType.READ)
	public Application displayHitsSince(long time) {
		LOGGER.warning("Displaying hits since " + time);

		Application app = new Application(appName);

		try {
			mongoDB.requestStart();
			DBCollection coll = mongoDB.getCollection(COLLECTION);

			BasicDBObject query = new BasicDBObject("time", new BasicDBObject(
					"$gt", time));
			DBCursor cur = coll.find(query);
			LOGGER.warning("Result set count: " + cur.count());

			try {
				while (cur.hasNext()) {
					DBObject result = cur.next();

					String gearId = (String) result.get("gear");

					// Get or create the gear for the application
					Gear gear = new Gear(gearId);
					if (!app.getChildren().contains(gear)) {
						app.getChildren().add(gear);
					} else {
						int index = app.getChildren().indexOf(gear);
						gear = app.getChildren().get(index);
					}

					String id = ((ObjectId) result.get("_id")).toString();
					Long timestamp = (Long) result.get("time");
					Integer hits = (Integer) result.get("hits");

					// Add the hits and timestamp to the gear
					gear.getChildren().add(
							new Hit(id, timestamp.longValue(), hits.intValue()));
				}
			} finally {
				cur.close();
			}
		} catch (Exception e) {
			// Try and re-establish the Mongo connection
			// initialize();

			throw new RuntimeException(e);
		} finally {
			mongoDB.requestDone();
		}

		LOGGER.fine("Application = " + app);

		return app;
	}

	/*
	 * Persist using the Timer service every second
	 */
	@Lock(LockType.WRITE)
	@Schedule(hour = "*", minute = "*", second = "*/2", persistent = false)
	public void persist() {
		if (hits > 0) {
			LOGGER.warning("Persisting " + hits + " to Mongo for gear " + gearId);

			try {
				mongoDB.requestStart();

				DBCollection coll = mongoDB.getCollection(COLLECTION);

				BasicDBObject doc = new BasicDBObject();
				doc.put("gear", gearId);
				doc.put("hits", hits);
				doc.put("time", System.currentTimeMillis());

				coll.insert(doc);
				
				// Reset the hit counter
				hits = 0;
				LOGGER.warning("Success");
			} finally {
				mongoDB.requestDone();
			}
		}
	}
	
	@Timeout
	public void timed() {
		// Just created to handle timeouts on the schedule calls
		// which can be ignored.
	}

	@Lock(LockType.WRITE)
	public void addHit() {
		hits++;
	}
}
