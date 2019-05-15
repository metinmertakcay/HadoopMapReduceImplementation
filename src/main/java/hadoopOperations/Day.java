package hadoopOperations;

public enum Day {
	MONDAY(1), TUESDAY(2), WEDNESDAY(3), THURSDAY(4), FRIDAY(5), SATURDAY(6), SUNDAY(7);

	private int id;

	Day(Integer id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

}
