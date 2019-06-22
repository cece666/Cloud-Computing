package common.messages;

public enum Source {
    CLIENT, ECS, SERVER;

    private static Source[] allValues = values();

    public static Source fromOrdinal(int statusInt) {
	if (statusInt >= allValues.length)
	    throw new IllegalArgumentException(String.format("%d is not a valid status code", statusInt));
	return allValues[statusInt];
    }
}
