# Design Consideration

## Previous step
- The previous step is to get the data from the Binance API using the websocket. The data is then handed over to the next step.

## Current step
- The current step is to process the data and reshape it into a format that is suitable for the downstream systems.

# Design
- Channel
There will be a series of channels that will be used to pass the data from one transformer to another. Each transformer will accept a number of channels as input and output a number of channels. So far, the system is closed and does not accept any input from the third-party sources except for the Binance API data. However, the system is designed to be open and can accept input from other sources(such as economics/market fundamentals) in the future. 

- Transformer
The transformer is the main component of the system. It will accept the data from the previous step and process it. The transformer will then output the data to the next step. The transformer will be designed to be stateless and can be easily scaled horizontally.

- ChannelData
The data that is passed through the channel will be in the form of ChannelData. The ChannelData is the interface that will be used to pass the data from one transformer to another. It could be casted to the specific data type that is required by the transformer. The cast will be done by the engineer who is responsible for the transformer.

Ultimately, the manager of the system is able to acquire the data after the data is processed by the transformer. The manager is stateful and will be able to decide whether to update some portion of the data or not and will pass it to the next step, which is the execution engine.

# Acknowledgement
To be honest, the design of this system is heavily inspired by the design of Metabit's internal data platform. They should receive the credit for the design of the system. 
However, I want to emphasize that this is not simple plagiarism, because any design of the same scene may have similarities. 