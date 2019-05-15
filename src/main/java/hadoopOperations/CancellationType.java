package hadoopOperations;

public enum CancellationType {
	NOT_CANCELLED(0), CANCELLED(1);

	private Integer id;

	CancellationType(Integer id) {
		this.id = id;
	}

	public Integer getId() {
		return id;
	}
}