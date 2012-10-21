package org.usergrid.vx.util;

import java.util.UUID;

import org.usergrid.persistence.entities.User;

public class UserFactory {

	public static User buildUser(UUID uuid) {
		User user = new User();
		user.setUsername("zznate"+uuid.toString());
		user.setEmail("nate" + uuid.toString() + "@apigee.com");
		user.setProperty("password", "password");
		return user;
	}
}
