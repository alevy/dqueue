class BlizzardLogger
  @@log_file_name = "operation_log"
  @@recovery_log_file_name = @@log_file_name + ".tmp"

  ADD_NODE = "ADD_NODE"
  START_ENQUEUE = "START_ENQUEUE"
  FINALIZE_ENQUEUE = "FINALIZE_ENQUEUE"
  ABORT_ENQUEUE = "ABORT_ENQUEUE"
  START_DEQUEUE = "START_DEQUEUE"
  FINALIZE_DEQUEUE = "FINALIZE_DEQUEUE"
  ABORT_DEQUEUE = "ABORT_DEQUEUE"
  DELIMITER = "|"
  
  def initialize
    @log_file = File.new(@@log_file_name, "a+")

  end
  
  # TODO checkpoints.
  
  def get_log_file
    File.copy(@@log_file_name, @@recovery_log_file_name)
    return File.new(@@recovery_log_file_name, "r")
  end
  
  def log_add_node(node_id, node)
    # node needs to be serializable to and from strings 
    # for this to be useful for recovery
    log ADD_NODE, node_id.to_s + DELIMITER + node.inspect#node["host"] + " " + node["port"] 
  end
  
  def log_enqueue_start(operation_id, enqueued_value)
    log_queue_operation START_ENQUEUE, operation_id, enqueued_value
  end
  
  def log_enqueue_finalize(operation_id, enqueued_value)
    log_queue_operation FINALIZE_ENQUEUE, operation_id, enqueued_value
  end
  
  def log_enqueue_abort(operation_id, enqueued_value)
    log_queue_operation ABORT_ENQUEUE, operation_id, enqueued_value
  end
  
  def log_dequeue_start(operation_id, dequeued_value)
    log_queue_operation START_DEQUEUE, operation_id, dequeued_value
  end
  
  def log_dequeue_finalize(operation_id, dequeued_value)
    log_queue_operation FINALIZE_DEQUEUE, operation_id, dequeued_value
  end

  def log_dequeue_abort(operation_id, dequeued_value)
    log_queue_operation ABORT_DEQUEUE, operation_id, dequeued_value
  end
    
  private
  def log(operation_type, message)
    @log_file.puts(operation_type.to_s + DELIMITER + message)
  end
  
  def log_queue_operation(operation_type, operation_id, value)
    log operation_type, operation_id.to_s + DELIMITER + value.to_s
  end
end