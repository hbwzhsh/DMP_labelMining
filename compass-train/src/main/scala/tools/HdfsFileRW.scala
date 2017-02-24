package tools

import org.apache.hadoop.fs._

class HdfsFileRW {
  @throws(classOf[IOException])
  def FileRead(fs: FileSystem, filename: String): BufferedReader = {
    val file: FileStatus = fs.getFileStatus(new Path(filename))
    val inputStream: FSDataInputStream = fs.open(file.getPath)
    val br = new BufferedReader(new InputStreamReader(inputStream))
    return br
  }

  @throws(classOf[IOException])
  def FileWrite(fs: FileSystem, filename: String, append:Boolean=true): BufferedWriter = {
    val outputStream: FSDataOutputStream = fs.create(new Path(filename),append)
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream))
    return bw
  }
}
