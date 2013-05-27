import org.slf4j.Logger
import org.slf4j.LoggerFactory

/* Using sqoop to export data into db */
Logger log = LoggerFactory.getLogger(this.getClass())

def command = """sqoop export --connect
 '${jdbcConnection}' --table ${tableName}
 --export-dir ${exportDir} --staging-table ${tableName}_stg --clear-staging-table"""


StringBuilder output = new StringBuilder()
StringBuilder err = new StringBuilder()

def process = command.execute();
process.consumeProcessOutput(output, err)
process.waitFor()

log.info(output.toString())
if (err.length() > 0) {
    log.error("Can't export HDFS data w/ command ${command} , error: ${err}")
}