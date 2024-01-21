from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsublite import AdminClient, Reservation
from google.cloud.pubsublite.types import CloudRegion, ReservationPath

# TODO(developer):
project_number = "hive-project-404608"
cloud_region = "us-central1"
zone_id = "c"
reservation_id = "reservation"
# Each unit of throughput capacity supports up to 1 MiB/s of published messages
# or 2 MiB/s of subscribed messages. Must be a positive integer.
throughput_capacity = 4

cloud_region = CloudRegion(cloud_region)
reservation_path = ReservationPath(project_number, cloud_region, reservation_id)

reservation = Reservation(
    name=str(reservation_path),
    throughput_capacity=throughput_capacity,
)

client = AdminClient(cloud_region)
try:
    response = client.create_reservation(reservation)
    print(f"{response.name} created successfully.")
except AlreadyExists:
    print(f"{reservation_path} already exists.")