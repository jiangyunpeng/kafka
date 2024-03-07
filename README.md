Apache Kafka
=================

1. 使用 2.2 分支
2. 导入IDEA需要配置Gradle。找到设置中的 [Build,Exception,Deployment]-> [Build,tool]-> [Gradle] ，Build and run使用IDEA内置的，不然每次在 IDEA 运行都会启动一个gradle，很烦！
3. ./gradlew core:build -x test
