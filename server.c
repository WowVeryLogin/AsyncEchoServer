#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>

#define MAXEVENTS 64
#define LOGFILEPATH "log_file.txt"

static int make_socket_non_blocking(int sfd) {
	int flags, s;
	flags = fcntl(sfd, F_GETFL, 0);
	if (flags == -1) {
		perror("fcntl");
		return -1;
	}

	flags |= O_NONBLOCK;
	s = fcntl(sfd, F_SETFL, flags);
	if (s == -1) {
		perror("fcntl");
		return -1;
	}

	return 0;
}

static int create_and_bind (char *port) {
	struct addrinfo hints;
	struct addrinfo *result, *rp;
	int s, sfd;

	memset(&hints, 0, sizeof (struct addrinfo));
	hints.ai_family = AF_UNSPEC;     /* Return IPv4 and IPv6 choices */
	hints.ai_socktype = SOCK_STREAM; /* TCP socket */
	hints.ai_flags = AI_PASSIVE;     /* All interfaces */

	s = getaddrinfo (NULL, port, &hints, &result);
	if (s != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror (s));
		return -1;
	}

	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sfd == -1)
			continue;

		if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &(int){ 1 }, sizeof(int)) < 0) {
			fprintf(stderr, "setsockopt(SO_REUSEADDR) failed\n");
			return -1;
		}

		s = bind(sfd, rp->ai_addr, rp->ai_addrlen);
		if (s == 0) {
		/* We managed to bind successfully! */
			break;
		}

		close(sfd);
	}

	if (rp == NULL) {
		fprintf(stderr, "Could not bind\n");
		return -1;
	}

	freeaddrinfo(result);

	return sfd;
}

struct message {
	char* data;
	int data_size;
	struct message *next;
};

struct client {
	int fd;
	struct message_queue *messages;
	struct client* next;
};

struct client_queue {
	ssize_t queue_len;
	struct client* begin;
	struct client* end;
};

struct message_queue {
	ssize_t queue_len;
	struct message* begin;
	struct message* end;
};

struct client* find_client(struct client_queue *queue, int fd) {
	if (!queue->queue_len) {
		return 0;
	}
	struct client* cur_client = queue->begin;
	while (cur_client) {
		if (cur_client->fd == fd) {
			return cur_client;
			break;
		}
		cur_client = cur_client->next;
	}
}

void dequeue_message(struct message_queue *queue);
void delete_all_messages(struct message_queue *queue) {
	while (queue->queue_len) {
		dequeue_message(queue);
	}
	free(queue);
}

void delete_client(struct client_queue *queue, int fd) {
	if (!queue->queue_len) {
		return;
	}
	struct client* cur_client = queue->begin;
	struct client* prev_client = 0;
	while (cur_client) {
		if (cur_client->fd == fd) {
			if (prev_client) {
				prev_client->next = cur_client->next;
			} else {
				queue->begin = cur_client->next;
			}
			delete_all_messages(cur_client->messages);
			free(cur_client);
			queue->queue_len--;
			break;
		}
		prev_client = cur_client;
		cur_client = cur_client->next;
	}
}

void enqueue_client(struct client_queue *queue, struct client *new_client) {
	if (queue->end) {
		queue->end->next = new_client;
		queue->end = new_client;
		queue->queue_len++;
	} else {
		queue->begin = queue->end = new_client;
		queue->queue_len = 1;
	}
}

struct message* pick_message(struct message_queue *queue) {
	if (!queue->queue_len) {
		return 0;
	}
	return queue->begin;
}

void dequeue_message(struct message_queue *queue) {
	if (!queue->queue_len) {
		return;
	}
	queue->queue_len--;
	struct message* tmp = queue->begin;
	queue->begin = queue->begin->next;
	free(tmp->data);
	free(tmp);
}

void enqueue_message(struct message_queue *queue, struct message *msg) {
	if (queue->end) {
		queue->end->next = msg;
		queue->end = msg;
		queue->queue_len++;
	} else {
		queue->begin = queue->end = msg;
		queue->queue_len = 1;
	}
}

struct client_queue* init_client_queue() {
	struct client_queue *clients = malloc(sizeof(struct client_queue));
	clients->begin = 0;
	clients->end = 0;
	clients->queue_len = 0;
	return clients;
}

void destroy_client_queue(struct client_queue* clients) {
	while (clients->queue_len) {
		delete_client(clients, clients->begin->fd);
	}
	free(clients);
}

void proccess_read_event(struct client_queue *clients, struct epoll_event* event, FILE* log) {
	int done = 0;
	while (1) {
		char buf[512];

		ssize_t count = read(event->data.fd, buf, sizeof(buf));
		if (count == -1) {
			/* If all data has been read, going back to the main loop */
			if (errno != EAGAIN) {
				perror ("read");
				done = 1;
			}
			break;
		}
		else if (count == 0) {
			/* End of file */
			done = 1;
			break;
		}

		/* Writing buffer to standard output */
		if (write(1, buf, count) == -1) {
			perror ("write");
			abort ();
		}
		if (fwrite(buf, sizeof(char), count, log) != count) {
			perror("write to log_file");
			abort();
		}

		struct client *cl = clients->begin;
		while(cl) {
			if (cl->fd != event->data.fd) {
				int done = 0;
				count = write (cl->fd, buf, count);
				if (count == -1) {
					if (errno != EAGAIN) {
						perror("write");
						done = 1;
					}
					struct message *msg = malloc(sizeof(struct message));
					msg->data = malloc(count);
					memcpy(msg->data, buf, count);
					msg->data_size = count;
					msg->next = 0;
					enqueue_message(cl->messages, msg);
				} else if (count == 0) {
					done = 1;
				}
				if (done) {
					printf ("Closed connection on descriptor %d\n", cl->fd);
					delete_client(clients, cl->fd);
					close (cl->fd);
				}
			}
			cl = cl->next;
		}
	}
	fflush(log);

	if (done) {
		printf ("Closed connection on descriptor %d\n", event->data.fd);
		delete_client(clients, event->data.fd);
		close (event->data.fd);
	}
}

void proccess_late_write_event(struct client_queue *clients, struct epoll_event* event) {
	int done = 0;
	while (1) {
		ssize_t count;
		struct client *cl = find_client(clients, event->data.fd);
		if (!cl) {
			break;
		}

		struct message* msg = pick_message(cl->messages);
		if (!msg) {
			break;
		}

		count = write(event->data.fd, msg->data, msg->data_size);
		if (count == -1) {
			/* If buffer is full, going back to the main loop. */
			if (errno != EAGAIN) {
				perror ("write");
				done = 1;
			}
			break;
		} else if (count == 0) {
			done = 1;
			break;
		}
		dequeue_message(cl->messages);
	}

	if (done) {
		printf ("Connection closed on descriptor %d\n", event->data.fd);
		delete_client(clients, event->data.fd);
		close (event->data.fd);
	}
}

void proccess_error_event(struct client_queue *clients, struct epoll_event* event) {
	fprintf (stderr, "epoll error\n");
	delete_client(clients, event->data.fd);
	close (event->data.fd);
}

void process_connection_event(int sfd, int efd, struct client_queue *clients) {
	while (1) {
		struct sockaddr in_addr;
		char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];

		socklen_t in_len = sizeof in_addr;
		int infd = accept(sfd, &in_addr, &in_len);
		if (infd == -1) {
			if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
				/* All connections has been processed */
				break;
			} else {
				perror("accept");
				break;
			}
		}

		int s = getnameinfo(&in_addr, in_len,
			hbuf, sizeof hbuf,
			sbuf, sizeof sbuf,
			NI_NUMERICHOST | NI_NUMERICSERV);
		if (s == 0) {
			printf("Connected on descriptor %d from host=%s, port=%s\n", infd, hbuf, sbuf);
		}

		if (make_socket_non_blocking (infd) == -1)
			abort();

		struct epoll_event event;
		event.data.fd = infd;
		event.events = EPOLLIN | EPOLLET | EPOLLOUT;
		if (epoll_ctl(efd, EPOLL_CTL_ADD, infd, &event) == -1) {
			perror("epoll_ctl");
			abort();
		}
		/* Queue for incoming messages */
		struct client* client = malloc(sizeof(struct client));
		client->fd = infd;
		struct message_queue* messages = malloc(sizeof(struct message_queue));
		messages->begin = 0;
		messages->end = 0;
		messages->queue_len = 0;
		client->messages = messages;
		client->next = 0;
		enqueue_client(clients, client);
	}
}

int main(int argc, char *argv[]) {
	struct client_queue *clients = init_client_queue();

	if (argc != 2) {
		fprintf(stderr, "Specify port: %s [port]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	FILE* log = fopen(LOGFILEPATH, "a");
	if (!log) {
		fprintf(stderr, "Cannot open log file: %s\n", LOGFILEPATH);
		abort();
	}

	int sfd = create_and_bind (argv[1]);
	if (sfd == -1)
		abort();

	if (make_socket_non_blocking (sfd) == -1)
		abort();

	if (listen(sfd, SOMAXCONN) == -1) {
		perror("listen");
		abort();
	}

	int efd = epoll_create1(0);
	if (efd == -1) {
		perror("epoll_create");
		abort();
	}

	struct epoll_event event;
	event.data.fd = sfd;
	event.events = EPOLLIN | EPOLLET | EPOLLOUT;
	if (epoll_ctl (efd, EPOLL_CTL_ADD, sfd, &event) == -1) {
		perror("epoll_ctl");
		abort();
	}

	/* Buffer where events are returned */
	struct epoll_event * events = calloc (MAXEVENTS, sizeof event);

	/* The event loop */
	while (1) {
		int n = epoll_wait (efd, events, MAXEVENTS, -1);
		for (int i = 0; i < n; i++) {
			if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) ||
					(!(events[i].events & EPOLLIN) && !(events[i].events & EPOLLOUT))) {
				proccess_error_event(clients, &events[i]);
				continue;
			} else if (sfd == events[i].data.fd) {
				process_connection_event(sfd, efd, clients);
				continue;
			} else if (events[i].events & EPOLLIN) {
				proccess_read_event(clients, &events[i], log);
			}

			if (events[i].events & EPOLLOUT) {
				proccess_late_write_event(clients, &events[i]);
			}
		}
	}

	free(events);
	destroy_client_queue(clients);
	close(sfd);
	fclose(log);

	return EXIT_SUCCESS;
}