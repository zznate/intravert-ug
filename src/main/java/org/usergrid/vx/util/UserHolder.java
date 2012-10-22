package org.usergrid.vx.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.usergrid.persistence.entities.User;

public class UserHolder implements Serializable, Iterable<User> {

	private List<User> users;
	
	public UserHolder() {
		users = new ArrayList<User>();
	}
	
	public void addUser(User user) {
		users.add(user);
	}

	@Override
	public Iterator<User> iterator() {

		return users.iterator();
	}
	
	
}
