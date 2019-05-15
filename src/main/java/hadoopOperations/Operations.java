package hadoopOperations;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Operations {
	private static Configuration configuration;
	private static URI uri;
	private static FileSystem fileSystem;
	private static FileSystem localSystem;
	private static Logger logger;

	/* initialize Haddop Cluster configuration */
	public Operations() {
		configuration = new Configuration();
		try {
			uri = new URI("hdfs://master:9000");
		} catch (URISyntaxException e) {
			logger.debug(e.getMessage(), e);
		}
		try {
			fileSystem = (DistributedFileSystem) FileSystem.get(uri, configuration);
			localSystem = FileSystem.getLocal(configuration);
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
		logger = LoggerFactory.getLogger(Operations.class.getName());
	}

	/* create a directory, if directory not exists in hdfs */
	public boolean mkdir(String dirName) {
		Path dir = new Path(dirName);
		try {
			if (!fileSystem.exists(dir)) {
				if (fileSystem.mkdirs(dir)) {
					return true;
				}
			}
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
		return false;
	}

	/* delete a file or directory, if exists */
	@SuppressWarnings("deprecation")
	public boolean deleteFileOrDir(String dirName) {
		Path dir = new Path(dirName);
		try {
			if (fileSystem.exists(dir)) {
				if (fileSystem.isDirectory(dir)) {
					fileSystem.delete(new Path(dirName), true);
				} else {
					fileSystem.delete(new Path(dirName), false);
				}
				return true;
			}
		} catch (IllegalArgumentException e) {
			logger.debug(e.getMessage(), e);
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
		return false;
	}

	/* list all path in hdfs */
	@SuppressWarnings("deprecation")
	public Path[] listFileInGivenPath(String dirPath)
			throws FileNotFoundException, IllegalArgumentException, IOException {
		Path path = new Path(dirPath);
		if (fileSystem.exists(path)) {
			if (fileSystem.isDirectory(path)) {
				FileStatus[] fileStatus = fileSystem.listStatus(path);
				Path[] paths = FileUtil.stat2Paths(fileStatus);
				return paths;
			}
		}
		return null;
	}

	/* detail information about file */
	public FileStatus getFileStatus(String filePath) {
		Path path = new Path(filePath);
		FileStatus fileStatus = null;
		try {
			fileStatus = fileSystem.getFileStatus(path);
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
		return fileStatus;
	}

	/* local to Hdfs */
	@SuppressWarnings("deprecation")
	public boolean localToHdfs(String localFilePath, String hdfsFilePath) {
		try {
			Path localPath = new Path(localFilePath);
			if (localSystem.exists(localPath)) {
				if (localSystem.isFile(localPath)) {
					FileStatus fileStatus = localSystem.getFileStatus(new Path(localFilePath));
					FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(hdfsFilePath));
					FSDataInputStream fsDataInputStream = localSystem.open(fileStatus.getPath());
					byte[] buffer = new byte[256];
					int len;
					while ((len = fsDataInputStream.read(buffer)) > 0) {
						fsDataOutputStream.write(buffer, 0, len);
					}
					fsDataInputStream.close();
					fsDataOutputStream.close();
					return true;
				}
			}
		} catch (IllegalArgumentException e) {
			logger.debug(e.getMessage(), e);
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
		return false;
	}

	/* HDFS to local */
	@SuppressWarnings("deprecation")
	public boolean hdfsToLocal(String hdfsFilePath, String localFilePath) {
		try {
			Path hdfsPath = new Path(hdfsFilePath);
			if (fileSystem.exists(hdfsPath)) {
				if (fileSystem.isFile(hdfsPath)) {
					FileStatus fileStatus = fileSystem.getFileStatus(hdfsPath);
					FSDataOutputStream fsDataOutputStream = localSystem.create(new Path(localFilePath));
					FSDataInputStream fsDataInputStream = fileSystem.open(fileStatus.getPath());
					byte[] buffer = new byte[256];
					int len;
					while ((len = fsDataInputStream.read(buffer)) > 0) {
						fsDataOutputStream.write(buffer, 0, len);
					}
					fsDataInputStream.close();
					fsDataOutputStream.close();
					return true;
				}
			}
		} catch (IllegalArgumentException e) {
			logger.debug(e.getMessage(), e);
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
		return false;
	}

	@SuppressWarnings("deprecation")
	public BlockLocation[] getFileBlockLocations(String hdfsFilePath) {
		BlockLocation[] blockLocation = null;
		try {
			Path path = new Path(hdfsFilePath);
			if (fileSystem.exists(path)) {
				if (fileSystem.isFile(path)) {
					FileStatus fileStatus = fileSystem.getFileStatus(path);
					blockLocation = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
				}
			}
		} catch (IllegalArgumentException e) {
			logger.debug(e.getMessage(), e);
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
		return blockLocation;
	}
}