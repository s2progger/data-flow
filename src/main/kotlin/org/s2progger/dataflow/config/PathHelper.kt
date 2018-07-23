package org.s2progger.dataflow.config

object PathHelper {
    fun appendToPath(dir: String) {
        var path = System.getProperty("java.library.path")
        path = "$dir;$path"
        System.setProperty("java.library.path", path)

        try {
            val sysPathsField = ClassLoader::class.java.getDeclaredField("sys_paths")

            sysPathsField.isAccessible = true
            sysPathsField.set(null, null)
        } catch (ex: Exception) {
            throw RuntimeException(ex)
        }

    }
}
