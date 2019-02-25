package com.github.adinauer.vertxchat

import ch.qos.logback.classic.Level
import com.google.gson.Gson
import io.vertx.core.AbstractVerticle
import io.vertx.core.buffer.Buffer
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.ErrorHandler
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.ext.web.handler.sockjs.SockJSHandler
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions
import io.vertx.ext.web.handler.sockjs.SockJSSocket
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.SubscriptionType
import java.util.*
import java.util.concurrent.ConcurrentHashMap


val pulsar = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build()

class CoroutineChatVerticle: CoroutineVerticle() {
    companion object {
        val log = LoggerFactory.getLogger(this::class.java)
    }

    val consumers = ConcurrentHashMap<String, Consumer<ByteArray>>()
    val sockets = ConcurrentHashMap<String, SockJSSocket>()

    override suspend fun start() {
        (org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as? ch.qos.logback.classic.Logger)?.level = Level.INFO

        val router = Router.router(vertx).also {
            val options = SockJSHandlerOptions().setHeartbeatInterval(2000)

            val sockJSHandler = SockJSHandler.create(vertx, options)

            try {
                sockJSHandler.socketHandler { socket ->
                    sockets.put(socket.writeHandlerID(), socket)
//          val sharedData = vertx.sharedData().getLocalMap<String, Consumer<ByteArray>>("subscriptions")
//                    val consumer = pulsar.newConsumer().topic("my-topic").subscriptionType(SubscriptionType.Shared).subscriptionName(UUID.randomUUID().toString()).messageListener { consumer, message ->
//                        log.info("received: ${String(message.data)}")
//                        socket.write(String(message.data))
//                    }.subscribe()
                    log.info("new socket [${socket.writeHandlerID()}]")

//          sharedData.put(socket.writeHandlerID(), consumer)
//                    consumers.put(socket.writeHandlerID(), consumer)

                    socket.handler { commandReceived(socket.writeHandlerID(), it) }
//                    socket.exceptionHandler { log.error("sockjs error", it) }
                }
            } catch(e: Exception) {
                log.error("xxx", e)
            }

            it.route("/ws/*").handler(sockJSHandler)
//      it.mountSubRouter("/api", chatApiRouter())
            it.route().failureHandler(errorHandler())
            it.route().handler(staticHandler())
        }

        vertx.createHttpServer().requestHandler(router).listen(8082)
    }

    private fun commandReceived(socketId: String, commandBuffer: Buffer) {
        try {
            val command = String(commandBuffer.bytes)
            val json = Gson().fromJson(command, Map::class.java)
            log.info("json: ${json}")

            // { "command": "join", "room": "xyz" }
            if (json.get("command") == "join") {
                val roomName = json.get("room") as? String ?: throw RuntimeException("no room name provided")
                val consumer = pulsar.newConsumer().topic(roomName).subscriptionType(SubscriptionType.Shared).subscriptionName(UUID.randomUUID().toString()).messageListener { consumer, message ->
                    log.info("received: ${String(message.data)}")
                    sockets.get(socketId)?.write(String(message.data))
                }.subscribe()

                consumers.put("${socketId}.${roomName}", consumer)
            }

            // { "command": "say", "room": "xyz", "message": "hello world" }
            if (json.get("command") == "say") {
                val roomName = json.get("room") as? String ?: throw RuntimeException("no room name provided")
                val message = json.get("message") as? String ?: throw RuntimeException("no message provided")

                pulsar.newProducer().topic(roomName).create().send(message.toByteArray())
            }

//            if (command.startsWith("/join")) {
//                val roomName = command.removePrefix("/join").trim()
//                val consumer = pulsar.newConsumer().topic(roomName).subscriptionType(SubscriptionType.Shared).subscriptionName(UUID.randomUUID().toString()).messageListener { consumer, message ->
//                    log.info("received: ${String(message.data)}")
//                    sockets.get(socketId)?.write(String(message.data))
//                }.subscribe()
//
//                consumers.put("${socketId}.${roomName}", consumer)
//            }

//            if (command.startsWith("/say")) {
//                val roomName = command.removePrefix("/say").trim().split(" ").first()
//                val message = command.removePrefix("/say").trim().split(" ").drop(1).joinToString(" ")
//
//                pulsar.newProducer().topic(roomName).create().send(message.toByteArray())
//            }
        } catch (t: Throwable) {
            log.error("zzz", t)
        }
    }

    private fun errorHandler(): ErrorHandler {
        return ErrorHandler.create(true)
    }

    private fun staticHandler(): StaticHandler {
        return StaticHandler.create()
                .setCachingEnabled(false)
    }


}

class ChatVerticle: AbstractVerticle() {
    companion object {
        val log = LoggerFactory.getLogger(this::class.java)
    }

    val consumers = ConcurrentHashMap<String, Consumer<ByteArray>>()
    val sockets = ConcurrentHashMap<String, SockJSSocket>()

    override fun start() {
        (org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as? ch.qos.logback.classic.Logger)?.level = Level.INFO

        val router = Router.router(vertx).also {
            val options = SockJSHandlerOptions().setHeartbeatInterval(2000)

            val sockJSHandler = SockJSHandler.create(vertx, options)

            try {
                sockJSHandler.socketHandler { socket ->
                    sockets.put(socket.writeHandlerID(), socket)
//          val sharedData = vertx.sharedData().getLocalMap<String, Consumer<ByteArray>>("subscriptions")
//                    val consumer = pulsar.newConsumer().topic("my-topic").subscriptionType(SubscriptionType.Shared).subscriptionName(UUID.randomUUID().toString()).messageListener { consumer, message ->
//                        log.info("received: ${String(message.data)}")
//                        socket.write(String(message.data))
//                    }.subscribe()
//                    log.info("new consumer [${socket.writeHandlerID()}]: ${consumer}")

//          sharedData.put(socket.writeHandlerID(), consumer)
//                    consumers.put(socket.writeHandlerID(), consumer)

                    socket.handler { commandReceived(socket.writeHandlerID(), it) }
                }
            } catch(e: Exception) {
                log.error("xxx", e)
            }

            it.route("/ws/*").handler(sockJSHandler)
//      it.mountSubRouter("/api", chatApiRouter())
            it.route().failureHandler(errorHandler())
            it.route().handler(staticHandler())
        }

        vertx.createHttpServer().requestHandler(router).listen(8082)
    }

    private fun commandReceived(socketId: String, commandBuffer: Buffer) {
        val command = String(commandBuffer.bytes)
        try {
            if (command.startsWith("/join")) {
                val roomName = command.removePrefix("/join").trim()
                val consumer = pulsar.newConsumer().topic(roomName).subscriptionType(SubscriptionType.Shared).subscriptionName(UUID.randomUUID().toString()).messageListener { consumer, message ->
                    log.info("received: ${String(message.data)}")
                    sockets.get(socketId)?.write(String(message.data))
                }.subscribe()

                consumers.put("${socketId}.${roomName}", consumer)
            }

            if (command.startsWith("/say")) {
                val roomName = command.removePrefix("/say").trim().split(" ").first()
                val message = command.removePrefix("/say").trim().split(" ").drop(1).joinToString(" ")

                pulsar.newProducer().topic(roomName).create().send(message.toByteArray())
            }
        } catch (t: Throwable) {
            log.error("zzz", t)
        }
    }

    private fun errorHandler(): ErrorHandler {
        return ErrorHandler.create(true)
    }

    private fun staticHandler(): StaticHandler {
        return StaticHandler.create()
                .setCachingEnabled(false)
    }
}



