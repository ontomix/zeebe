/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zeebe.model.bpmn.impl;

import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.impl.instance.DefinitionsImpl;
import io.zeebe.model.bpmn.instance.Definitions;
import org.camunda.bpm.model.xml.ModelBuilder;
import org.camunda.bpm.model.xml.impl.ModelImpl;
import org.camunda.bpm.model.xml.impl.ModelInstanceImpl;
import org.camunda.bpm.model.xml.instance.DomDocument;

/**
 * The Bpmn Model
 *
 * @author Daniel Meyer
 */
public class BpmnModelInstanceImpl extends ModelInstanceImpl implements BpmnModelInstance {

  public BpmnModelInstanceImpl(ModelImpl model, ModelBuilder modelBuilder, DomDocument document) {
    super(model, modelBuilder, document);
  }

  @Override
  public Definitions getDefinitions() {
    return (DefinitionsImpl) getDocumentElement();
  }

  @Override
  public void setDefinitions(Definitions definitions) {
    setDocumentElement(definitions);
  }

  @Override
  public BpmnModelInstance clone() {
    return new BpmnModelInstanceImpl(model, modelBuilder, document.clone());
  }
}
