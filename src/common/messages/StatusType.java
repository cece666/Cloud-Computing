package common.messages;

public enum StatusType {
	IDENTIFY, /* Message use purely for identifying the sender */

	// Informational statuses
	INFO, /* Message from server to inform client */
	DONE, /* Command from ECS was successfully executed */
	FAIL, /* Fail - fail message */

	// Client -> Server requests
	GET, /* Get - request */
	PUT, /* Put - request */
	TIMED_PUT, /* TimedPut - request */
	DELETE, /* Delete - request */
	LOGIN, /* Login - request */
	SIGN_UP, /* Sign up - request */

	// Server -> Client response statuses
	GET_ERROR, /* requested tuple (i.e. value) not found */
	GET_SUCCESS, /* requested tuple (i.e. value) found */

	PUT_SUCCESS, /* Put - request successful, tuple inserted */
	PUT_UPDATE, /* Put - request successful, i.e. value updated */
	PUT_ERROR, /* Put - request not successful */

	TIMED_PUT_SUCCESS, /* TimedPut - request successful, tuple inserted */
	TIMED_PUT_UPDATE, /* TimedPut - request successful, i.e. value updated */
	TIMED_PUT_ERROR, /* TimedPut - request not successful */

	DELETE_SUCCESS, /* Delete - request successful */
	DELETE_ERROR, /* Delete - request successful */

	LOGIN_SUCCESS, /* Login - successfully login */
	LOGIN_ERROR, /* Login - failed to login */

	SIGN_UP_SUCCESS, /* Sign up - successfully register */
	SIGN_UP_ERROR, /* Sign up - failed to sign up */

	SERVER_STOPPED, /* Server is stopped, no requests are processed */
	SERVER_WRITE_LOCK, /* Server locked for out, only get possible */
	SERVER_NOT_RESPONSIBLE, /* Request sent to the wrong server */
	DEAD_SERVER, /* ECS informed of the dead server */

	// ECS -> Server commands
	INIT, /* Initialize the start with necessary information */
	START, /* Make servers ready for clients' requests */
	STOP, /* Make servers stop processing client's requests */
	SHUTDOWN, /* Shutdown servers */
	LOCK_WRITE, /* Lock the server so client can only read */
	UNLOCK_WRITE, /* Unlock the server so the client can write to it */
	UPDATE, /* Update the metadata of the server */

	MOVE_DATA, /* Move data from one to another */
	MOVE_DATA_SUCCESS, MOVE_DATA_FAIL,

	REPLICATE, /* Replicate the data of the sender */
	REPLICATE_SUCCESS, REPLICATE_FAIL,

	ADD_USER, ADD_USER_SUCCESS, ADD_USER_ERROR,

	GET_ACCESS_DENIED, UPDATE_ACCESS_DENIED, DELETE_ACCESS_DENIED,

	PING, PONG; /* Messages for failure detection */

	private static StatusType[] allValues = values();

	public static StatusType fromOrdinal(int statusInt) {
		if (statusInt >= allValues.length)
			throw new IllegalArgumentException(String.format("%d is not a valid status code", statusInt));
		return allValues[statusInt];
	}
}