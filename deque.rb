# Daniel Suskin
# CSE550
# double-ended queue

# PRETTY SURE WE DON'T NEED THIS
class Deque
    @head
    @tail
    
    def initialize()
        @head = nil
        @tail = nil
    end
    
    def enqueue(newTailData)
        newTail = DequeNode.new
        newTail.setData newTailData
        
        if !@tail.nil?
            newTail.setPrev @tail
            @tail.setNext newTail
        else
            @head = newTail
        end
        
        @tail = newTail
    end
    
    def enqueueAsHead(newHeadData)
        newHead = DequeNode.new
        newHead.setData newHeadData
        
        if !@tail.nil?
            newHead.setNext @head
            @head.setNext newHead
        else
            @tail = newHead
        end
        
        @head = newHead
    end
    
    def dequeue()
        returnData = nil
        
        if !@head.nil?
            returnData = @head.data
            @head = @head.next
            
            if !@head.nil?
                @head.setPrev nil
            end
        end
        
        return returnData
    end
    
    def dequeueTail()
        returnData = nil
        
        if !@tail.nil?
            returnData = @tail.data
            @tail = @tail.prev
            
            if !@tail.nil?
                @tail.setNext nil
            end
        end
        
        return returnData
    end
end

class DequeNode
    def initialize()
        @next = nil
        @prev = nil
        @data = nil
    end
    
    def next
        return @next
    end
    
    def setNext(newNext)
        @next = newNext
    end
    
    def prev
        return @prev
    end
    
    def setPrev(newPrev)
        @prev = newPrev
    end
     
    def data
        return @data
    end
    
    def setData(newData)
        @data = newData
    end
end