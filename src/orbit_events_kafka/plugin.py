# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Optional Core plugin that registers and owns one Kafka event transport."""

from orbit import Application
from orbit.plugins import Plugin, PluginMetadata

from orbit_events_kafka.transport import KafkaEventTransport

KAFKA_EVENT_TRANSPORT_KEY = "orbit.events.transport.kafka"


class KafkaTransportPlugin(Plugin):
    """Register a configured transport and close its clients during Core shutdown."""

    metadata = PluginMetadata(
        name="orbit-events-kafka",
        version="0.1.0a1",
        capabilities=frozenset({"events.kafka"}),
    )

    def __init__(self, transport: KafkaEventTransport) -> None:
        """Take lifecycle ownership of a transport configured by the application."""
        if not isinstance(transport, KafkaEventTransport):
            raise TypeError("KafkaTransportPlugin requires a KafkaEventTransport instance.")
        self.transport = transport

    def setup(self, application: Application) -> None:
        """Expose the transport under its stable optional-package dependency key."""
        if not isinstance(application, Application):
            raise TypeError("KafkaTransportPlugin requires an Orbit Application.")
        application.container.register_instance(KAFKA_EVENT_TRANSPORT_KEY, self.transport)

    async def deactivate(self) -> None:
        """Stop consumers and producer on normal shutdown or startup rollback."""
        await self.transport.close()


__all__ = ["KAFKA_EVENT_TRANSPORT_KEY", "KafkaTransportPlugin"]
