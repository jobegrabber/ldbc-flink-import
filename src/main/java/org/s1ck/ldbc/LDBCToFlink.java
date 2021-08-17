/*
 * This file is part of ldbc-flink-import.
 *
 * ldbc-flink-import is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ldbc-flink-import is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *
 * You should have received a copy of the GNU General Public License
 * along with ldbc-flink-import. If not, see <http://www.gnu.org/licenses/>.
 */

package org.s1ck.ldbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.s1ck.ldbc.functions.EdgeLineReader;
import org.s1ck.ldbc.functions.PropertyLineReader;
import org.s1ck.ldbc.functions.PropertyValueGroupReducer;
import org.s1ck.ldbc.functions.VertexLineReader;
import org.s1ck.ldbc.functions.VertexPropertyGroupCoGroupReducer;
import org.s1ck.ldbc.tuples.LDBCEdge;
import org.s1ck.ldbc.tuples.LDBCMultiValuedProperty;
import org.s1ck.ldbc.tuples.LDBCProperty;
import org.s1ck.ldbc.tuples.LDBCVertex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.s1ck.ldbc.LDBCConstants.*;

/**
 * Main class to read LDBC output into Apache Flink.
 */
public class LDBCToFlink {

  /** Logger */
  private static final Logger LOG = Logger.getLogger(LDBCToFlink.class);

  /** Flink execution environment */
  private final ExecutionEnvironment env;

  /** Hadoop Configuration */
  private final Configuration conf;

  /** Directory, where the LDBC output is stored. */
  private final String ldbcDirectory;

  /**
   * Defines how tokens are separated in a filename. For example in
   * "comment_0_0.csv" the tokens are separated by "_".
   */
  private final Pattern fileNameTokenDelimiter;

  /** List of vertex files */
  private final List<FileStatus> vertexFilePaths;

  /** List of edge files */
  private final List<FileStatus> edgeFilePaths;

  /** List of property files */
  private final List<FileStatus> propertyFilePaths;

  /**
   * Maps a vertex class (e.g. Person, Comment) to a unique identifier.
   */
  private final Map<String, Long> vertexClassToClassIDMap;

  /**
   * Used to create vertex class IDs.
   */
  private long nextVertexClassID = 0L;

  /**
   * Creates a new parser instance.
   *
   * @param ldbcDirectory path to LDBC output
   * @param env Flink execution environment
   */
  public LDBCToFlink(String ldbcDirectory, ExecutionEnvironment env) {
    this(ldbcDirectory, env, new Configuration());
  }

  /**
   * Creates a new parser instance.
   *
   * @param ldbcDirectory path to LDBC output
   * @param env Flink execution environment
   * @param conf Hadoop cluster configuration
   */
  public LDBCToFlink(String ldbcDirectory, ExecutionEnvironment env,
    Configuration conf) {
    if (ldbcDirectory == null || "".equals(ldbcDirectory)) {
      throw new IllegalArgumentException("LDBC directory must not be null or empty");
    }
    if (env == null) {
      throw new IllegalArgumentException("Flink Execution Environment must not be null");
    }
    if (conf == null) {
      throw new IllegalArgumentException("Hadoop Configuration must not  be null");
    }
    LOG.info("conf = " + conf.toString());
    this.ldbcDirectory = ldbcDirectory;
    this.vertexFilePaths = Lists.newArrayList();
    this.edgeFilePaths = Lists.newArrayList();
    this.propertyFilePaths = Lists.newArrayList();
    this.env = env;
    this.conf = conf;
    this.vertexClassToClassIDMap = Maps.newHashMap();
    fileNameTokenDelimiter = Pattern.compile(FILENAME_TOKEN_DELIMITER);
    init();
  }

  /**
   * Parses and transforms the LDBC vertex files to {@link LDBCVertex} tuples.
   *
   * @return DataSet containing all vertices in the LDBC graph
   */
  public DataSet<LDBCVertex> getVertices() throws IOException {
    LOG.info("Reading vertices");
    final List<DataSet<LDBCVertex>> vertexDataSets =
      Lists.newArrayListWithCapacity(vertexFilePaths.size());
    for (FileStatus fileStatus : vertexFilePaths) {
      if (fileStatus.isFile())
        vertexDataSets.add(readVertexFile(fileStatus.getPath(), false));
      else {
        for (FileStatus childStatus : FileSystem.get(conf).listStatus(fileStatus.getPath())) {
          if (childStatus.isFile()) {
            Path childPath = childStatus.getPath();
            if (childPath.getName().endsWith("csv"))
              vertexDataSets.add(readVertexFile(childPath, true));
          }
        }
      }
    }

    DataSet<LDBCVertex> vertices = unionDataSets(vertexDataSets);

    vertices = addMultiValuePropertiesToVertices(vertices);

    return vertices;
  }

  /**
   * Parses and transforms the LDBC edge files to {@link LDBCEdge} tuples.
   *
   * @return DataSet containing all edges in the LDBC graph.
   */
  public DataSet<LDBCEdge> getEdges() throws IOException {
    LOG.info("Reading edges");
    List<DataSet<LDBCEdge>> edgeDataSets =
      Lists.newArrayListWithCapacity(edgeFilePaths.size());
    for (FileStatus filePath : edgeFilePaths) {
      if (filePath.isFile())
        edgeDataSets.add(readEdgeFile(filePath.getPath(), false));
      else {
        for (FileStatus childStatus : FileSystem.get(conf).listStatus(filePath.getPath())) {
          if (childStatus.isFile()) {
            Path childPath = childStatus.getPath();
            if (childPath.getName().endsWith("csv"))
              edgeDataSets.add(readEdgeFile(childPath, true));
          }
        }
      }
    }

    return DataSetUtils.zipWithUniqueId(unionDataSets(edgeDataSets))
      .map(new MapFunction<Tuple2<Long, LDBCEdge>, LDBCEdge>() {
        @Override
        public LDBCEdge map(Tuple2<Long, LDBCEdge> tuple) throws Exception {
          tuple.f1.setEdgeId(tuple.f0);
          return tuple.f1;
        }
      }).withForwardedFields("f0");
  }

  private DataSet<LDBCVertex> addMultiValuePropertiesToVertices(
    DataSet<LDBCVertex> vertices) throws IOException {
    DataSet<LDBCMultiValuedProperty> groupedProperties = getProperties()
      // group properties by vertex id and property key
      .groupBy(0, 1)
        // and build tuples containing vertex id, property key and value list
      .reduceGroup(new PropertyValueGroupReducer());

    // co group vertices and property groups and update vertices
    return vertices.coGroup(groupedProperties).where(0).equalTo(0)
      .with(new VertexPropertyGroupCoGroupReducer());
  }

  private DataSet<LDBCProperty> getProperties() throws IOException {
    LOG.info("Reading multi valued properties");
    List<DataSet<LDBCProperty>> propertyDataSets =
      Lists.newArrayListWithCapacity(propertyFilePaths.size());

    for (FileStatus filePath : propertyFilePaths) {
      if (filePath.isFile())
        propertyDataSets.add(readPropertyFile(filePath.getPath(), false));
      else {
        for (FileStatus childStatus : FileSystem.get(conf).listStatus(filePath.getPath())) {
          if (childStatus.isFile()) {
            Path childPath = childStatus.getPath();
            if (childPath.getName().endsWith("csv"))
              propertyDataSets.add(readPropertyFile(childPath, true));
          }
        }
      }
    }

    return unionDataSets(propertyDataSets);
  }

  private long getVertexClassCount() {
    return vertexFilePaths.size();
  }

  private <T> DataSet<T> unionDataSets(List<DataSet<T>> dataSets) {
    DataSet<T> finalDataSet = null;
    boolean first = true;
    for (DataSet<T> dataSet : dataSets) {
      if (first) {
        finalDataSet = dataSet;
        first = false;
      } else {
        finalDataSet = finalDataSet.union(dataSet);
      }
    }
    return finalDataSet;
  }

  private DataSet<LDBCVertex> readVertexFile(Path filePath, boolean isDirectory) {
    LOG.info("Reading vertices from " + filePath.toString());

    String vertexClass = getVertexClass(getFileName(isDirectory
    ? getFileName(filePath.getParent().getName())
    : filePath.getName()).toLowerCase());
    Long vertexClassID = getVertexClassId(vertexClass);
    Long classCount = (long) vertexFilePaths.size();

    LOG.info(String.format("vertex class: %s vertex class ID: %d", vertexClass,
      vertexClassID));

    String[] vertexClassFields = null;
    FieldType[] vertexClassFieldTypes = null;
    switch (vertexClass) {
    case VERTEX_CLASS_COMMENT:
      vertexClassFields = VERTEX_CLASS_COMMENT_FIELDS;
      vertexClassFieldTypes = VERTEX_CLASS_COMMENT_FIELD_TYPES;
      break;
    case VERTEX_CLASS_FORUM:
      vertexClassFields = VERTEX_CLASS_FORUM_FIELDS;
      vertexClassFieldTypes = VERTEX_CLASS_FORUM_FIELD_TYPES;
      break;
    case VERTEX_CLASS_ORGANISATION:
      vertexClassFields = VERTEX_CLASS_ORGANISATION_FIELDS;
      vertexClassFieldTypes = VERTEX_CLASS_ORGANISATION_FIELD_TYPES;
      break;
    case VERTEX_CLASS_PERSON:
      vertexClassFields = VERTEX_CLASS_PERSON_FIELDS;
      vertexClassFieldTypes = VERTEX_CLASS_PERSON_FIELD_TYPES;
      break;
    case VERTEX_CLASS_PLACE:
      vertexClassFields = VERTEX_CLASS_PLACE_FIELDS;
      vertexClassFieldTypes = VERTEX_CLASS_PLACE_FIELD_TYPES;
      break;
    case VERTEX_CLASS_POST:
      vertexClassFields = VERTEX_CLASS_POST_FIELDS;
      vertexClassFieldTypes = VERTEX_CLASS_POST_FIELD_TYPES;
      break;
    case VERTEX_CLASS_TAG:
      vertexClassFields = VERTEX_CLASS_TAG_FIELDS;
      vertexClassFieldTypes = VERTEX_CLASS_TAG_FIELD_TYPES;
      break;
    case VERTEX_CLASS_TAGCLASS:
      vertexClassFields = VERTEX_CLASS_TAGCLASS_FIELDS;
      vertexClassFieldTypes = VERTEX_CLASS_TAGCLASS_FIELD_TYPES;
      break;
    }
    return env.readTextFile(filePath.toString(), "UTF-8").flatMap(
      new VertexLineReader(vertexClassID, vertexClass, vertexClassFields,
        vertexClassFieldTypes, classCount));
  }

  private DataSet<LDBCEdge> readEdgeFile(Path filePath, boolean isDirectory) {
    LOG.info("Reading edges from " + filePath.toString());

    String fileName = getFileName(isDirectory ? filePath.getParent().getName() : filePath.getName());
    String edgeClass = getEdgeClass(fileName);
    String sourceVertexClass = getSourceVertexClass(fileName);
    String targetVertexClass = getTargetVertexClass(fileName);
    Long sourceVertexClassId = getVertexClassId(sourceVertexClass);
    Long targetVertexClassId = getVertexClassId(targetVertexClass);
    Long vertexClassCount = getVertexClassCount();

    String[] edgeClassFields = null;
    FieldType[] edgeClassFieldTypes = null;
    switch (edgeClass) {
    case EDGE_CLASS_KNOWS:
      edgeClassFields = EDGE_CLASS_KNOWS_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_KNOWS_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_TYPE:
      edgeClassFields = EDGE_CLASS_HAS_TYPE_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_TYPE_FIELD_TYPES;
      break;
    case EDGE_CLASS_IS_LOCATED_IN:
      edgeClassFields = EDGE_CLASS_IS_LOCATED_IN_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_IS_LOCATED_IN_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_INTEREST:
      edgeClassFields = EDGE_CLASS_HAS_INTEREST_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_INTEREST_FIELD_TYPES;
      break;
    case EDGE_CLASS_REPLY_OF:
      edgeClassFields = EDGE_CLASS_REPLY_OF_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_REPLY_OF_FIELD_TYPES;
      break;
    case EDGE_CLASS_STUDY_AT:
      edgeClassFields = EDGE_CLASS_STUDY_AT_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_STUDY_AT_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_MODERATOR:
      edgeClassFields = EDGE_CLASS_HAS_MODERATOR_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_MODERATOR_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_MEMBER:
      edgeClassFields = EDGE_CLASS_HAS_MEMBER_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_MEMBER_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_TAG:
      edgeClassFields = EDGE_CLASS_HAS_TAG_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_TAG_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_CREATOR:
      edgeClassFields = EDGE_CLASS_HAS_CREATOR_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_CREATOR_FIELD_TYPES;
      break;
    case EDGE_CLASS_WORK_AT:
      edgeClassFields = EDGE_CLASS_WORK_AT_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_WORK_AT_FIELD_TYPES;
      break;
    case EDGE_CLASS_CONTAINER_OF:
      edgeClassFields = EDGE_CLASS_CONTAINER_OF_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_CONTAINER_OF_FIELD_TYPES;
      break;
    case EDGE_CLASS_IS_PART_OF:
      edgeClassFields = EDGE_CLASS_IS_PART_OF_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_IS_PART_OF_FIELD_TYPES;
      break;
    case EDGE_CLASS_IS_SUBCLASS_OF:
      edgeClassFields = EDGE_CLASS_IS_SUBCLASS_OF_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_IS_SUBCLASS_OF_FIELD_TYPES;
      break;
    case EDGE_CLASS_LIKES:
      edgeClassFields = EDGE_CLASS_LIKES_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_LIKES_FIELD_TYPES;
      break;
    }

    return env.readTextFile(filePath.toString(), "UTF-8").flatMap(
      new EdgeLineReader(edgeClass, edgeClassFields, edgeClassFieldTypes,
        sourceVertexClassId, sourceVertexClass, targetVertexClassId,
        targetVertexClass, vertexClassCount));
  }

  private DataSet<LDBCProperty> readPropertyFile(Path filePath, boolean isDirectory) {
    LOG.info("Reading properties from " + filePath.toString());

    String fileName = getFileName(isDirectory ? filePath.getParent().getName() : filePath.getName());
    String propertyClass = getPropertyClass(fileName);
    String vertexClass = getVertexClass(fileName);
    Long vertexClassId = getVertexClassId(vertexClass);
    Long vertexClassCount = getVertexClassCount();

    String[] propertyClassFields = null;
    FieldType[] propertyClassFieldTypes = null;

    switch (propertyClass) {
    case PROPERTY_CLASS_EMAIL:
      propertyClassFields = PROPERTY_CLASS_EMAIL_FIELDS;
      propertyClassFieldTypes = PROPERTY_CLASS_EMAIL_FIELD_TYPES;
      break;
    case PROPERTY_CLASS_SPEAKS:
      propertyClassFields = PROPERTY_CLASS_SPEAKS_FIELDS;
      propertyClassFieldTypes = PROPERTY_CLASS_SPEAKS_FIELD_TYPES;
      break;
    }

    return env.readTextFile(filePath.toString(), "UTF-8").flatMap(
      new PropertyLineReader(propertyClass, propertyClassFields,
        propertyClassFieldTypes, vertexClass, vertexClassId, vertexClassCount));
  }

  private String getFileName(String filePath) {
    return filePath
      .substring(filePath.lastIndexOf(System.getProperty("file.separator")) + 1,
        filePath.length());
  }

  private String getVertexClass(String fileName) {
    int indexOfFilenameTokenDelimiter = fileName.indexOf(FILENAME_TOKEN_DELIMITER);
    return indexOfFilenameTokenDelimiter != -1
            ? fileName.substring(0, indexOfFilenameTokenDelimiter)
            : fileName;
  }

  private String getEdgeClass(String fileName) {
    return fileNameTokenDelimiter.split(fileName)[1];
  }

  private String getPropertyClass(String fileName) {
    return fileNameTokenDelimiter.split(fileName)[1];
  }

  private String getSourceVertexClass(String fileName) {
    return fileNameTokenDelimiter.split(fileName)[0];
  }

  private String getTargetVertexClass(String fileName) {
    return fileNameTokenDelimiter.split(fileName)[2];
  }

  private List<Path> getCSVFilesInDirectory(FileSystem fileSystem, Path dir) throws IOException {
    List<Path> paths = new ArrayList<>();
    FileStatus[] fileStates = fileSystem.listStatus(dir);
    for (FileStatus childStatus : fileStates) {
      Path childPath = childStatus.getPath();
      if (childStatus.isFile() && childPath.getName().endsWith("csv")) {
        paths.add(childPath);
      }
    }
    return paths;
  }

  private boolean isVertexPath(FileStatus fileStatus) {
    String name = fileStatus.getPath().getName();
    return (fileStatus.isFile() && name.split(FILENAME_TOKEN_DELIMITER).length == 3)
            || (fileStatus.isDirectory() && name.split(FILENAME_TOKEN_DELIMITER).length == 1);
  }

  private boolean isEdgePath(FileStatus fileStatus) {
    String name = fileStatus.getPath().getName();
    return ((fileStatus.isFile() &&
      name.split(FILENAME_TOKEN_DELIMITER).length == 5) || (fileStatus.isDirectory() &&
            name.split(FILENAME_TOKEN_DELIMITER).length == 3)) &&
            !name.contains(PROPERTY_CLASS_EMAIL) &&
            !name.contains(PROPERTY_CLASS_SPEAKS);
  }


  private boolean isPropertyPath(FileStatus fileStatus) {
    String name = fileStatus.getPath().getName();
    return  name.contains(PROPERTY_CLASS_EMAIL) || name.contains(PROPERTY_CLASS_SPEAKS);
  }

  private Long getVertexClassId(String vertexClass) {
    Long vertexClassID;
    if (vertexClassToClassIDMap.containsKey(vertexClass)) {
      vertexClassID = vertexClassToClassIDMap.get(vertexClass);
    } else {
      vertexClassID = nextVertexClassID++;
      vertexClassToClassIDMap.put(vertexClass, vertexClassID);
    }
    return vertexClassID;
  }

  private void init() {
    if (ldbcDirectory.startsWith("hdfs://")) {
      initFromHDFS();
    } else {
      throw new UnsupportedOperationException("Can only load from HDFS");
    }
  }

  private void initFromHDFS() {
    try {
      FileSystem fs = FileSystem.get(conf);
      Path p = new Path(ldbcDirectory);
      if (!fs.exists(p) || !fs.isDirectory(p)) {
        throw new IllegalArgumentException(
          String.format("%s does not exist or is not a directory", ldbcDirectory));
      }
      FileStatus[] fileStates = fs.listStatus(p);
      for (FileStatus fileStatus : fileStates) {
          if (isVertexPath(fileStatus)) {
            vertexFilePaths.add(fileStatus);
          }
        else if (isEdgePath(fileStatus)) {
          edgeFilePaths.add(fileStatus);
        }
        else if (isPropertyPath(fileStatus)) {
          propertyFilePaths.add(fileStatus);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
