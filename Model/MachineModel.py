class MachineModel:

  def __init__(self, 
              memory     = None,
              cpu        = None,
              storage    = None,
              processor  = None,
              gen        = None,
              fam        = None,
              cs         = None,
              net        = None,
              parc       = None,
              ebt        = None,
              colection_data = None
            ) -> None:
              self.memory         = memory        
              self.cpu            = cpu           
              self.storage        = storage       
              self.processor      = processor     
              self.gen            = gen           
              self.fam            = fam           
              self.cs             = cs            
              self.net            = net           
              self.parc           = parc          
              self.ebt            = ebt           
              self.colection_data = colection_data
      