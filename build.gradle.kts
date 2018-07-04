import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    kotlin("jvm") version "1.2.51"
}

group = "com.simontwogood"
version = "0.1"

repositories {
    mavenCentral()
}

dependencies {
    compile(kotlin("stdlib"))

    compile("io.github.microutils:kotlin-logging:1.5.4")
    compile("org.slf4j:slf4j-simple:1.7.25")
    compile("com.github.salomonbrys.kotson:kotson:2.5.0")
    compile("com.h2database:h2:1.4.197")
    compile("com.microsoft.sqlserver:mssql-jdbc:6.4.0.jre8")
    compile(files("lib/ojdbc8.jar"))

    testCompile("junit", "junit", "4.12")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

// TODO: Look into adding parcl for compiling to an exe [https://github.com/mini2Dx/parcl]
